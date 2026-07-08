use async_graphql::{Context, Error, Object, Result};
use bson::Bson;
use bson::Uuid;
use futures::TryStreamExt;
use graphql_client::GraphQLQuery;
use graphql_client::Response;
use mongodb::{
    bson::{doc, DateTime},
    Collection, Database,
};
use serde::Deserialize;
use serde::Serialize;
use std::any::type_name;
use tracing::instrument;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;
use std::time::Instant;

use crate::{
    authorization::{authorize_user, AuthorizedUserHeader},
    event::model::order_dto::OrderDTO,
};

use super::{
    model::{
        foreign_types::{
            Coupon, Discount, ProductVariant, ProductVariantVersion, ShipmentMethod, TaxRate,
            TaxRateVersion, UserAddress,
        },
        order::{Order, OrderStatus},
        order_item::OrderItem,
        payment_authorization::PaymentAuthorization,
        user::User,
    },
    mutation_input_structs::{CreateOrderInput, OrderItemInput, PlaceOrderInput},
    query::{query_object, query_objects},
};

const PENDING_TIMEOUT: Duration = Duration::new(3600, 0);

/// Describes GraphQL order mutations.
pub struct Mutation;

#[Object]
impl Mutation {
    /// Creates an order with `OrderStatus::Pending`.
    #[instrument(skip(self, ctx, input), fields(user_id = %input.user_id))]
    async fn create_order<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(desc = "CreateOrderInput")] input: CreateOrderInput,
    ) -> Result<Order> {
        authorize_user(&ctx, Some(input.user_id))?;
        let db_client = ctx.data::<Database>()?;
        let collection: Collection<Order> = db_client.collection::<Order>("orders");
        validate_order_input(db_client, &input).await?;
        let current_timestamp = DateTime::now();
        let internal_order_items: Vec<OrderItem> =
            create_internal_order_items(&ctx, &input, current_timestamp).await?;
        let shipment_address = UserAddress::from(input.shipment_address_id);
        let invoice_address = UserAddress::from(input.invoice_address_id);
        let compensatable_order_amount =
            calculate_compensatable_order_amount(&internal_order_items);
        let order = Order {
            _id: Uuid::new(),
            user: User::from(input.user_id),
            created_at: current_timestamp,
            order_status: OrderStatus::Pending,
            placed_at: None,
            rejection_reason: None,
            internal_order_items,
            shipment_address,
            invoice_address,
            compensatable_order_amount,
            payment_information_id: input.payment_information_id,
            vat_number: input.vat_number,
        };
        insert_order_in_mongodb(&collection, order).await
    }

    /// Places an existing order by changing its status to `OrderStatus::Placed`.
    ///
    /// Adds optional payment authorization input to order DTO when placing order.
    async fn place_order<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(desc = "PlaceOrderInput")] input: PlaceOrderInput,
    ) -> Result<Order> {
        let db_client = ctx.data::<Database>()?;
        let collection: Collection<Order> = db_client.collection::<Order>("orders");
        let mut order = query_object(&collection, input.id).await?;
        authorize_user(&ctx, Some(order.user._id))?;
        let payment_authorization = build_payment_authorization(&input);
        set_status_placed(&collection, input.id).await?;
        order = query_object(&collection, input.id).await?;
        let order_dto = OrderDTO::try_from((order.clone(), payment_authorization))?;
        send_order_created_event(order_dto).await?;
        Ok(order)
    }
}

/// Builds payment authorization from place order input.
///
/// `input` - The place order input to build the payment authorization from.
fn build_payment_authorization(input: &PlaceOrderInput) -> Option<PaymentAuthorization> {
    input
        .payment_authorization
        .clone()
        .and_then(|definitely_payment_authorization| {
            Option::<PaymentAuthorization>::from(definitely_payment_authorization)
        })
}

/// Inserts order in MongoDB and returns the order itself.
///
/// * `collection` - MongoDB collection to insert order in.
/// * `order` - Order to insert.
#[instrument(skip(collection, order), fields(order_id = %order._id))]
async fn insert_order_in_mongodb(collection: &Collection<Order>, order: Order) -> Result<Order> {
    match collection.insert_one(order, None).await {
        Ok(result) => {
            let id = uuid_from_bson(result.inserted_id)?;
            query_object(&collection, id).await
        }
        Err(_) => Err(Error::new("Adding order failed in MongoDB.")),
    }
}

/// Calculates the total compensatable amount of all order items in the input by summing up their `compensatable_amount` attributes.
///
/// `order_items` - Order items to calculate compensatable amount for.
fn calculate_compensatable_order_amount(order_items: &Vec<OrderItem>) -> u64 {
    order_items
        .iter()
        .map(|order_item| order_item.compensatable_amount)
        .sum()
}

/// Extracts UUID from Bson.
///
/// Creating a order returns a UUID in a Bson document. This function helps to extract the UUID.
///
/// * `bson` - BSON document to extract UUID from.
fn uuid_from_bson(bson: Bson) -> Result<Uuid> {
    match bson {
        Bson::Binary(id) => Ok(id.to_uuid()?),
        _ => {
            let message = format!(
                "Returned id: `{}` needs to be a Binary in order to be parsed as a Uuid",
                bson
            );
            Err(Error::new(message))
        }
    }
}

/// Sets the status of an order to `OrderStatus::Placed`.
/// Checks if pending order is still valid before setting `OrderStatus::Placed`.
/// Rejects order if timestamp of placement exceeds `PENDING_TIMEOUT` in relation to the order creation timestamp.
///
/// * `collection` - MongoDB collection to update.
/// * `id` - UUID of order to set the order status to placed.
async fn set_status_placed(collection: &Collection<Order>, id: Uuid) -> Result<()> {
    let current_timestamp_system_time = SystemTime::now();
    let order = query_object(&collection, id).await?;
    let order_created_at_system_time = order.created_at.to_system_time();
    if order_created_at_system_time + PENDING_TIMEOUT >= current_timestamp_system_time {
        match order.order_status {
            OrderStatus::Pending => {
                let current_timestamp = DateTime::from(current_timestamp_system_time);
                set_status_placed_in_mongodb(&collection, id, current_timestamp).await
            }
            _ => {
                let message = format!("`{:?}` must be `OrderStatus::Pending` to be able to be placed. Order was already placed or rejected.", order.order_status);
                Err(Error::new(message))
            }
        }
    } else {
        set_status_rejected_in_mongodb(&collection, id).await
    }
}

/// Updates order to `OrderStatus::Placed` in MongoDB.
///
/// * `collection` - MongoDB collection to set the order status as placed in.
/// * `id` - UUID of order to set the order status to placed.
/// * `current_timestamp` - Timestamp of order placement.
async fn set_status_placed_in_mongodb(
    collection: &Collection<Order>,
    id: Uuid,
    current_timestamp: DateTime,
) -> Result<()> {
    let result = collection
        .update_one(
            doc! {"_id": id },
            doc! {"$set": {"order_status": OrderStatus::Placed, "placed_at": current_timestamp}},
            None,
        )
        .await;
    if let Err(_) = result {
        let message = format!("Placing order of id: `{}` failed in MongoDB.", id);
        return Err(Error::new(message));
    }
    Ok(())
}

/// Updates order to `OrderStatus::Rejected` in MongoDB.
///
/// This function always returns an error.
///
/// `collection` - MongoDB collection to modify the order status in.
/// `id` - UUID of order to set the status to rejected.
async fn set_status_rejected_in_mongodb(collection: &Collection<Order>, id: Uuid) -> Result<()> {
    let result = collection
        .update_one(
            doc! {"_id": id },
            doc! {"$set": {"order_status": OrderStatus::Rejected}},
            None,
        )
        .await;
    match result {
        Ok(_) => {
            let message = format!(
                "Order of id: `{}` was rejected as it is `OrderStatus::Pending` for too long.",
                id
            );
            return Err(Error::new(message));
        }
        Err(_) => {
            let message = format!("Order should be rejected as it is `OrderStatus::Pending` for too long. Rejecting order of id: `{}` failed in MongoDB.", id);
            return Err(Error::new(message));
        }
    }
}

/// Checks if foreign types exist (MongoDB database populated with events).
#[instrument(skip(db_client, input), fields(user_id = %input.user_id))]
async fn validate_order_input(db_client: &Database, input: &CreateOrderInput) -> Result<()> {
    let user_collection: mongodb::Collection<User> = db_client.collection::<User>("users");
    validate_object(&user_collection, input.user_id).await?;
    validate_order_items(&db_client, &input.order_item_inputs).await?;
    validate_addresses(&db_client, &input).await?;
    Ok(())
}

/// Checks if all order item parameters are the system (MongoDB database populated with events).
///
/// Used before creating orders.
#[instrument(skip(db_client, order_item_inputs))]
async fn validate_order_items(
    db_client: &Database,
    order_item_inputs: &BTreeSet<OrderItemInput>,
) -> Result<()> {
    let shipment_method_collection: mongodb::Collection<ShipmentMethod> =
        db_client.collection::<ShipmentMethod>("shipment_methods");
    let shipment_method_ids = order_item_inputs
        .iter()
        .map(|order_item_input| order_item_input.shipment_method_id)
        .collect();
    validate_objects(&shipment_method_collection, shipment_method_ids).await?;
    validate_coupons(&db_client, &order_item_inputs).await?;
    Ok(())
}

/// Checks if coupons are in the system (MongoDB database populated with events).
///
/// Used before creating orders.
#[instrument(skip(db_client, order_item_inputs))]
async fn validate_coupons(
    db_client: &Database,
    order_item_inputs: &BTreeSet<OrderItemInput>,
) -> Result<()> {
    let coupon_collection: mongodb::Collection<Coupon> = db_client.collection::<Coupon>("coupons");
    let coupon_ids: Vec<Uuid> = order_item_inputs
        .iter()
        .map(|order_item_input| order_item_input.coupon_ids.clone())
        .flatten()
        .collect();
    validate_objects(&coupon_collection, coupon_ids).await
}

/// Checks if addresses are registered under the user (MongoDB database populated with events).
///
/// Used before creating orders.
#[instrument(skip(db_client, input), fields(user_id = %input.user_id))]
async fn validate_addresses(db_client: &Database, input: &CreateOrderInput) -> Result<()> {
    let user_collection: mongodb::Collection<User> = db_client.collection::<User>("users");
    validate_user_address(&user_collection, input.shipment_address_id, input.user_id).await?;
    validate_user_address(&user_collection, input.invoice_address_id, input.user_id).await
}

/// Creates order items from order item inputs.
///
/// Used before creating orders.
/// Each order can only contain an order item with a specific product variant once.
#[instrument(skip(ctx, input, current_timestamp), fields(user_id = %input.user_id))]
async fn create_internal_order_items<'a>(
    ctx: &Context<'a>,
    input: &CreateOrderInput,
    current_timestamp: DateTime,
) -> Result<Vec<OrderItem>> {
    let db_client = ctx.data::<Database>()?;
    let authorized_header = ctx.data::<AuthorizedUserHeader>()?;
    let (
        counts_by_product_variant_ids,
        order_item_inputs_by_product_variant_ids,
        product_variants_by_product_variant_ids,
        product_variant_versions_by_product_variant_ids,
        tax_rate_versions_by_product_variant_ids,
        discounts_by_product_variant_ids,
    ) = query_or_obtain_order_item_attributes(authorized_header, input, db_client).await?;
    let internal_order_items = zip_to_internal_order_items(
        order_item_inputs_by_product_variant_ids,
        product_variants_by_product_variant_ids,
        product_variant_versions_by_product_variant_ids,
        tax_rate_versions_by_product_variant_ids,
        counts_by_product_variant_ids,
        discounts_by_product_variant_ids,
        current_timestamp,
    )?;
    Ok(internal_order_items)
}

/// Queries or obtains the attributes necessary for order item construction.
#[instrument(skip(authorized_header, input, db_client), fields(user_id = %input.user_id))]
async fn query_or_obtain_order_item_attributes(
    authorized_header: &AuthorizedUserHeader,
    input: &CreateOrderInput,
    db_client: &Database,
) -> Result<
    (
        HashMap<Uuid, u64>,
        HashMap<Uuid, OrderItemInput>,
        HashMap<Uuid, ProductVariant>,
        HashMap<Uuid, ProductVariantVersion>,
        HashMap<Uuid, TaxRateVersion>,
        HashMap<Uuid, BTreeSet<Discount>>,
    ),
    Error,
> {
    let (counts_by_product_variant_ids, order_item_inputs_by_product_variant_ids) =
        query_counts_by_product_variant_ids(authorized_header, &input).await?;
    let product_variant_ids: Vec<Uuid> = counts_by_product_variant_ids.keys().cloned().collect();
    let product_variants_by_product_variant_ids: HashMap<Uuid, ProductVariant> =
        query_product_variants_by_product_variant_ids(db_client, &product_variant_ids).await?;
    let product_variant_versions_by_product_variant_ids =
        query_product_variant_versions_by_product_variant_ids(
            &product_variants_by_product_variant_ids,
        )
        .await;
    check_product_variant_availability(&product_variant_ids, &counts_by_product_variant_ids)
        .await?;
    let tax_rate_versions_by_product_variant_ids = query_tax_rate_versions_by_product_variant_ids(
        db_client,
        &product_variant_versions_by_product_variant_ids,
    )
    .await?;
    let discounts_by_product_variant_ids = query_discounts_by_product_variant_ids(
        input.user_id,
        &order_item_inputs_by_product_variant_ids,
        &product_variant_ids,
        &product_variant_versions_by_product_variant_ids,
        &counts_by_product_variant_ids,
    )
    .await?;
    let _shipment_fees = query_shipment_fees(
        &order_item_inputs_by_product_variant_ids,
        &product_variant_versions_by_product_variant_ids,
        &counts_by_product_variant_ids,
    )
    .await?;
    Ok((
        counts_by_product_variant_ids,
        order_item_inputs_by_product_variant_ids,
        product_variants_by_product_variant_ids,
        product_variant_versions_by_product_variant_ids,
        tax_rate_versions_by_product_variant_ids,
        discounts_by_product_variant_ids,
    ))
}

/// Zips hash maps which contain the required attributes for construction to order items.
fn zip_to_internal_order_items(
    order_item_inputs_by_product_variant_ids: HashMap<Uuid, OrderItemInput>,
    product_variants_by_product_variant_ids: HashMap<Uuid, ProductVariant>,
    product_variant_versions_by_product_variant_ids: HashMap<Uuid, ProductVariantVersion>,
    tax_rate_versions_by_product_variant_ids: HashMap<Uuid, TaxRateVersion>,
    counts_by_product_variant_ids: HashMap<Uuid, u64>,
    discounts_by_product_variant_ids: HashMap<Uuid, BTreeSet<Discount>>,
    current_timestamp: DateTime,
) -> Result<Vec<OrderItem>> {
    product_variants_by_product_variant_ids
        .iter()
        .map(|(id, product_variant)| {
            let order_item_input_error =
                build_hash_map_error(&order_item_inputs_by_product_variant_ids, *id);
            let product_variant_version_error =
                build_hash_map_error(&product_variant_versions_by_product_variant_ids, *id);
            let tax_rate_version_error =
                build_hash_map_error(&tax_rate_versions_by_product_variant_ids, *id);
            let count_error = build_hash_map_error(&counts_by_product_variant_ids, *id);
            let discount_error = build_hash_map_error(&discounts_by_product_variant_ids, *id);
            let order_item_input = order_item_inputs_by_product_variant_ids
                .get(id)
                .ok_or(order_item_input_error)?;
            let product_variant_version = product_variant_versions_by_product_variant_ids
                .get(id)
                .ok_or(product_variant_version_error)?;
            let tax_rate_version = tax_rate_versions_by_product_variant_ids
                .get(id)
                .ok_or(tax_rate_version_error)?;
            let count = counts_by_product_variant_ids.get(id).ok_or(count_error)?;
            let internal_discounts = discounts_by_product_variant_ids
                .get(id)
                .ok_or(discount_error)?;
            let order_item = OrderItem::new(
                order_item_input,
                product_variant,
                product_variant_version,
                tax_rate_version,
                *count,
                internal_discounts,
                current_timestamp,
            );
            Ok(order_item)
        })
        .collect::<Result<Vec<OrderItem>>>()
}

// Defines a custom scalar from GraphQL schema.
type _Any = Representation;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schemas_repo/inventory.graphql",
    query_path = "queries/get_unreserved_product_item_counts.graphql",
    response_derives = "Debug"
)]
/// GraphQL query generated by client library.
struct GetUnreservedProductItemCounts;
#[derive(Serialize, Debug)]

/// Input type for a GraphQL entity resolver query.
struct Representation {
    __typename: String,
    id: String,
}

/// Checks if product items are available in the inventory service.
#[instrument(skip(product_variant_ids, counts_by_product_variant_ids), fields(product_variant_count = product_variant_ids.len()))]
async fn check_product_variant_availability(
    product_variant_ids: &Vec<Uuid>,
    counts_by_product_variant_ids: &HashMap<Uuid, u64>,
) -> Result<()> {
    let representations = product_variant_ids
        .iter()
        .cloned()
        .map(|id| Representation {
            __typename: "ProductVariant".to_string(),
            id: id.to_string(),
        })
        .collect();
    let variables = get_unreserved_product_item_counts::Variables { representations };

    let request_body = GetUnreservedProductItemCounts::build_query(variables);
    let client = reqwest::Client::new();

    let res = client
        .post("http://localhost:3500/v1.0/invoke/inventory/method/graphql")
        .json(&request_body)
        .send()
        .await?;
    let response_body: Response<get_unreserved_product_item_counts::ResponseData> =
        res.json().await?;
    let response_data: get_unreserved_product_item_counts::ResponseData =
        response_body.data.ok_or(Error::new(
            "Response data of `check_product_variant_availability` query is empty.",
        ))?;
    let stock_counts_by_product_variant_ids =
        build_stock_counts_by_product_variant_from_response_data(response_data)?;
    calculate_availability_of_product_variant_ids(
        &stock_counts_by_product_variant_ids,
        &counts_by_product_variant_ids,
    )
}

/// Remaps the result type of the GraphQL `_entities` query retrieving stock counts for product variants.
fn build_stock_counts_by_product_variant_from_response_data(
    response_data: get_unreserved_product_item_counts::ResponseData,
) -> Result<HashMap<Uuid, u64>> {
    response_data
        .entities
        .into_iter()
        .map(|maybe_product_variant_enum| {
            let message = format!("Response data of `check_product_variant_availability` query could not be parsed, `maybe_product_variant_enum` is `None`");
            let product_variant_enum = maybe_product_variant_enum.ok_or(Error::new(message))?;
            let stock_counts_by_product_variant: Result<(Uuid, u64)> = match product_variant_enum {
                get_unreserved_product_item_counts::GetUnreservedProductItemCountsEntities::ProductVariant(product_variant) => {
                    let stock_count = u64::try_from(product_variant.inventory_count)?;
                    Ok(
                        (
                            product_variant.id,
                            stock_count
                        )
                    )
                }
                get_unreserved_product_item_counts::GetUnreservedProductItemCountsEntities::ProductItem => todo!(),
            };
            stock_counts_by_product_variant
        }).collect()
}

/// Calculates the availability based on the actual and expected stock counts based on the product variant UUIDs.
///
/// The expected amount or more product items need to be in stock for a product variant to be counted as available.
/// All product variants need to be available for this function to pass without an error.
fn calculate_availability_of_product_variant_ids(
    stock_counts_by_product_variant_ids: &HashMap<Uuid, u64>,
    expected_stock_counts_by_product_variant_ids: &HashMap<Uuid, u64>,
) -> Result<()> {
    let availabilites: Vec<bool> = expected_stock_counts_by_product_variant_ids
        .iter()
        .map(|(id, expected_count)| {
            let error = build_hash_map_error(expected_stock_counts_by_product_variant_ids, *id);
            let count = stock_counts_by_product_variant_ids.get(id).ok_or(error)?;
            Ok(*count >= *expected_count)
        })
        .collect::<Result<Vec<bool>>>()?;
    match availabilites
        .into_iter()
        .all(|is_available| is_available == true)
    {
        true => Ok(()),
        false => Err(Error::new(
            "Not all requested product variants are available.",
        )),
    }
}

// Defines a custom scalar from GraphQL schema.
type UUID = Uuid;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schemas_repo/shoppingcart.graphql",
    query_path = "queries/get_shopping_cart_product_variant_ids_and_counts.graphql",
    response_derives = "Debug"
)]
/// GraphQL query generated by client library.
struct GetShoppingCartProductVariantIdsAndCounts;

/// Queries product variants from shopping cart item ids from shopping cart service.
#[instrument(skip(authorized_user_header, input), fields(user_id = %input.user_id))]
async fn query_counts_by_product_variant_ids(
    authorized_user_header: &AuthorizedUserHeader,
    input: &CreateOrderInput,
) -> Result<(HashMap<Uuid, u64>, HashMap<Uuid, OrderItemInput>)> {
    let representations = vec![Representation {
        __typename: "User".to_string(),
        id: input.user_id.to_string(),
    }];
    let variables = get_shopping_cart_product_variant_ids_and_counts::Variables { representations };

    let request_body = GetShoppingCartProductVariantIdsAndCounts::build_query(variables);
    let client = reqwest::Client::new();

    let authorized_user_header_string = serde_json::to_string(authorized_user_header)?;
    let res = client
        .post("http://localhost:3500/v1.0/invoke/shoppingcart/method/")
        .json(&request_body)
        .header("Authorized-User", authorized_user_header_string)
        .send()
        .await?;
    let response_body: Response<get_shopping_cart_product_variant_ids_and_counts::ResponseData> =
        res.json().await?;
    let message = "Response data of `query_counts_by_product_variant_ids` query is empty.";
    let mut response_data: get_shopping_cart_product_variant_ids_and_counts::ResponseData =
        response_body.data.ok_or(Error::new(message))?;
    let shopping_cart_response_data = response_data.entities.remove(0).ok_or(message)?;

    let ids_and_counts_by_shopping_cart_item_ids =
        into_ids_and_counts_by_shopping_cart_item_ids(shopping_cart_response_data)?;
    let counts_by_product_variant_ids = build_counts_by_product_variant_ids(
        &input.order_item_inputs,
        &ids_and_counts_by_shopping_cart_item_ids,
    )?;
    let order_item_inputs_by_product_variant_ids = build_order_item_inputs_by_product_variant_ids(
        &input.order_item_inputs,
        &ids_and_counts_by_shopping_cart_item_ids,
    )?;
    Ok((
        counts_by_product_variant_ids,
        order_item_inputs_by_product_variant_ids,
    ))
}

// Unwraps enum and maps the result to a hash map of shopping cart item ids as keys and `(product_variant_id, count)` as values.
fn into_ids_and_counts_by_shopping_cart_item_ids(
    ids_and_counts_enum: get_shopping_cart_product_variant_ids_and_counts::GetShoppingCartProductVariantIdsAndCountsEntities,
) -> Result<HashMap<Uuid, (Uuid, u64)>> {
    let message = format!("`ids_and_counts_enum: get_shopping_cart_product_variant_ids_and_counts::GetShoppingCartProductVariantIdsAndCountsEntities` does not contain a `get_shopping_cart_product_variant_ids_and_counts::GetShoppingCartProductVariantIdsAndCountsEntities::User`, but is another entity: `{:?}`", ids_and_counts_enum);
    match ids_and_counts_enum {
        get_shopping_cart_product_variant_ids_and_counts::GetShoppingCartProductVariantIdsAndCountsEntities::User(user) => {
            let ids_and_counts_by_shopping_cart_item_ids = user.shoppingcart.shoppingcart_items.nodes.iter().map(|shoppingcart_item|
                (shoppingcart_item.id, (shoppingcart_item.product_variant.id, shoppingcart_item.count as u64))
            ).collect();
            Ok(ids_and_counts_by_shopping_cart_item_ids)
        }
        _ => Err(Error::new(message))?,
    }
}

/// Filters shopping cart items: `ids_and_counts` to map to `order_item_inputs`.
/// Builds hash map which maps product variant ids to counts.
fn build_counts_by_product_variant_ids(
    order_item_inputs: &BTreeSet<OrderItemInput>,
    ids_and_counts: &HashMap<Uuid, (Uuid, u64)>,
) -> Result<HashMap<Uuid, u64>> {
    order_item_inputs
        .iter()
        .map(|order_item_input| {
            let id_and_count_ref = ids_and_counts.get(&order_item_input.shopping_cart_item_id);
            let id_and_count = id_and_count_ref.and_then(|(id, count)| Some((*id, *count)));
            let error =
                build_hash_map_error(ids_and_counts, order_item_input.shopping_cart_item_id);
            id_and_count.ok_or(error)
        })
        .collect()
}

/// Filters shopping cart items: `ids_and_counts` to map to `order_item_inputs`.
/// Builds hash map which maps product variant ids to order item inputs.
fn build_order_item_inputs_by_product_variant_ids(
    order_item_inputs: &BTreeSet<OrderItemInput>,
    ids_and_counts: &HashMap<Uuid, (Uuid, u64)>,
) -> Result<HashMap<Uuid, OrderItemInput>> {
    order_item_inputs
        .iter()
        .map(|order_item_input| {
            let id_and_count_ref = ids_and_counts.get(&order_item_input.shopping_cart_item_id);
            let id_and_count =
                id_and_count_ref.and_then(|(id, _)| Some((*id, order_item_input.clone())));
            let error =
                build_hash_map_error(ids_and_counts, order_item_input.shopping_cart_item_id);
            id_and_count.ok_or(error)
        })
        .collect()
}

/// Obtains product variants from product variant UUIDs.
///
/// Filters product variants which are non-publicly-visible.
#[instrument(skip(db_client, product_variant_ids), fields(product_variant_count = product_variant_ids.len()))]
async fn query_product_variants_by_product_variant_ids(
    db_client: &Database,
    product_variant_ids: &Vec<Uuid>,
) -> Result<HashMap<Uuid, ProductVariant>> {
    let collection: Collection<ProductVariant> =
        db_client.collection::<ProductVariant>("product_variants");
    let product_variants_by_product_variant_ids_unfiltered =
        query_objects(&collection, product_variant_ids).await?;
    let product_variants_by_product_variant_ids =
        product_variants_by_product_variant_ids_unfiltered
            .into_iter()
            .filter(|(_, p)| p.is_publicly_visible)
            .collect();
    Ok(product_variants_by_product_variant_ids)
}

/// Obtains current product variant versions using product variants.
#[instrument(skip(product_variants_by_product_variant_ids), fields(product_variant_count = product_variants_by_product_variant_ids.len()))]
async fn query_product_variant_versions_by_product_variant_ids(
    product_variants_by_product_variant_ids: &HashMap<Uuid, ProductVariant>,
) -> HashMap<Uuid, ProductVariantVersion> {
    let product_variant_versions_by_product_variant_ids: HashMap<Uuid, ProductVariantVersion> =
        product_variants_by_product_variant_ids
            .iter()
            .map(|(id, p)| (*id, p.current_version))
            .collect();
    product_variant_versions_by_product_variant_ids
}

/// Obtains current tax rate version for tax rate in product variant versions.
#[instrument(skip(db_client, product_variant_versions_by_product_variant_ids), fields(product_variant_count = product_variant_versions_by_product_variant_ids.len()))]
async fn query_tax_rate_versions_by_product_variant_ids(
    db_client: &Database,
    product_variant_versions_by_product_variant_ids: &HashMap<Uuid, ProductVariantVersion>,
) -> Result<HashMap<Uuid, TaxRateVersion>> {
    let collection: Collection<TaxRate> = db_client.collection::<TaxRate>("tax_rates");
    let tax_rate_ids: Vec<Uuid> = product_variant_versions_by_product_variant_ids
        .iter()
        .map(|(_, p)| p.tax_rate_id)
        .collect();
    let tax_rates = query_objects(&collection, &tax_rate_ids).await?;
    let tax_rate_versions_by_product_variant_ids = product_variant_versions_by_product_variant_ids
        .iter()
        .map(|(id, p)| {
            let error = build_hash_map_error(&tax_rates, *id);
            let tax_rate = tax_rates.get(&p.tax_rate_id).ok_or(error)?;
            Ok((*id, tax_rate.current_version))
        })
        .collect::<Result<HashMap<Uuid, TaxRateVersion>>>()?;
    Ok(tax_rate_versions_by_product_variant_ids)
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schemas_repo/discount.graphql",
    query_path = "queries/get_discounts.graphql",
    response_derives = "Debug"
)]
/// GraphQL query generated by client library.
pub struct GetDiscounts;

/// Queries discounts for coupons from discount service.
#[instrument(skip(order_item_inputs_by_product_variant_ids, product_variant_ids, product_variant_versions_by_product_variant_ids, counts_by_product_variant_ids), fields(user_id = %user_id, product_variant_count = product_variant_ids.len()))]
async fn query_discounts_by_product_variant_ids(
    user_id: Uuid,
    order_item_inputs_by_product_variant_ids: &HashMap<Uuid, OrderItemInput>,
    product_variant_ids: &Vec<Uuid>,
    product_variant_versions_by_product_variant_ids: &HashMap<Uuid, ProductVariantVersion>,
    counts_by_product_variant_ids: &HashMap<Uuid, u64>,
) -> Result<HashMap<Uuid, BTreeSet<Discount>>> {
    let find_applicable_discounts_product_variant_input =
        build_find_applicable_discounts_product_variant_input(
            order_item_inputs_by_product_variant_ids,
            product_variant_ids,
            counts_by_product_variant_ids,
        )?;
    let order_amount = calculate_order_amount(&product_variant_versions_by_product_variant_ids);
    let find_applicable_discounts_input = build_find_applicable_discounts_input(
        user_id,
        find_applicable_discounts_product_variant_input,
        order_amount,
    );
    let variables = get_discounts::Variables {
        find_applicable_discounts_input,
    };
    let request_body = GetDiscounts::build_query(variables);
    let client = reqwest::Client::new();

    let res = client
        .post("http://localhost:3500/v1.0/invoke/discount/method/graphql")
        .json(&request_body)
        .send()
        .await?;
    let response_body: Response<get_discounts::ResponseData> = res.json().await?;
    let response_data: get_discounts::ResponseData = response_body.data.ok_or(Error::new(
        "Response data of `query_discounts` query is empty.",
    ))?;
    build_discounts_from_response_data(response_data, product_variant_ids)
}

/// Remaps the result type of the GraphQL `findApplicableDiscounts` query to the the according product variants.
/// Converts the GraphQL client library generated discounts to the internally used discounts, which are GraphQL `SimpleObject`.
fn build_discounts_from_response_data(
    response_data: get_discounts::ResponseData,
    product_variant_ids: &Vec<Uuid>,
) -> Result<HashMap<Uuid, BTreeSet<Discount>>> {
    let graphql_client_lib_discounts: HashMap<
        Uuid,
        get_discounts::GetDiscountsFindApplicableDiscounts,
    > = remap_discounts_to_product_variants(
        response_data.find_applicable_discounts,
        &product_variant_ids,
    )?;
    let simple_object_discounts = convert_graphql_client_lib_discounts_to_simple_object_discounts(
        graphql_client_lib_discounts,
    );
    Ok(simple_object_discounts)
}

/// Builds `get_discounts::FindApplicableDiscountsInput`, which is the following struct:
///
/// ```
/// pub struct FindApplicableDiscountsInput {
///     #[serde(rename = "orderAmount")]
///     pub order_amount: Int,
///     #[serde(rename = "productVariants")]
///     pub product_variants: Vec<FindApplicableDiscountsProductVariantInput>,
///     #[serde(rename = "userId")]
///     pub user_id: UUID,
/// }
/// ```
///
/// Describes the order amount, which is the sum of all product variant version prices, a vector of `get_discounts::FindApplicableDiscountsProductVariantInput` and the user which the discounts are be queried for.
fn build_find_applicable_discounts_input(
    user_id: Uuid,
    find_applicable_discounts_product_variant_input: Vec<
        get_discounts::FindApplicableDiscountsProductVariantInput,
    >,
    order_amount: i64,
) -> get_discounts::FindApplicableDiscountsInput {
    let find_applicable_discounts_input = get_discounts::FindApplicableDiscountsInput {
        user_id,
        product_variants: find_applicable_discounts_product_variant_input,
        order_amount,
    };
    find_applicable_discounts_input
}

/// Builds part of the `get_discounts::FindApplicableDiscountsInput`, which is a vector of the following struct:
///
/// ```
/// pub struct FindApplicableDiscountsProductVariantInput {
///     pub product_variant_id: Uuid,
///     pub count: u64,
///     pub coupon_ids: HashSet<Uuid>,
/// }
/// ```
///
/// Describes product variant ids, the count of items planned to order and the coupons, which should be applied.
fn build_find_applicable_discounts_product_variant_input(
    order_item_inputs_by_product_variant_ids: &HashMap<Uuid, OrderItemInput>,
    product_variant_ids: &Vec<Uuid>,
    counts_by_product_variant_ids: &HashMap<Uuid, u64>,
) -> Result<Vec<get_discounts::FindApplicableDiscountsProductVariantInput>> {
    let find_applicable_discounts_product_variant_input: Vec<
        get_discounts::FindApplicableDiscountsProductVariantInput,
    > = product_variant_ids
        .iter()
        .map(|id| {
            let counts_error = build_hash_map_error(counts_by_product_variant_ids, *id);
            let count = counts_by_product_variant_ids.get(id).ok_or(counts_error)?;
            let order_item_error =
                build_hash_map_error(order_item_inputs_by_product_variant_ids, *id);
            let coupon_ids = order_item_inputs_by_product_variant_ids
                .get(id)
                .ok_or(order_item_error)?
                .coupon_ids
                .iter()
                .cloned()
                .collect();
            let find_applicable_discounts_product_variant_input =
                get_discounts::FindApplicableDiscountsProductVariantInput {
                    product_variant_id: *id,
                    count: i64::try_from(*count)?,
                    coupon_ids,
                };
            Ok::<get_discounts::FindApplicableDiscountsProductVariantInput, Error>(
                find_applicable_discounts_product_variant_input,
            )
        })
        .collect::<Result<Vec<get_discounts::FindApplicableDiscountsProductVariantInput>>>()?;
    Ok(find_applicable_discounts_product_variant_input)
}

/// Remaps the result type of the GraphQL `findApplicableDiscounts` query to the the according product variants.
fn remap_discounts_to_product_variants(
    discounts_for_product_variants_response_data: Vec<
        get_discounts::GetDiscountsFindApplicableDiscounts,
    >,
    product_variant_ids: &Vec<Uuid>,
) -> Result<HashMap<Uuid, get_discounts::GetDiscountsFindApplicableDiscounts>> {
    let mut discounts_for_product_variants: HashMap<
        Uuid,
        get_discounts::GetDiscountsFindApplicableDiscounts,
    > = discounts_for_product_variants_response_data
        .into_iter()
        .fold(
        HashMap::new(),
        |mut map: HashMap<Uuid, get_discounts::GetDiscountsFindApplicableDiscounts>,
         discount_for_product_variant: get_discounts::GetDiscountsFindApplicableDiscounts| {
            map.insert(
                discount_for_product_variant.product_variant_id,
                discount_for_product_variant,
            );
            map
        },
    );
    product_variant_ids.iter().map(|id| {
        let message = format!("Product variant of UUID: `{}` is not contained in the result which `findApplicableDiscounts` provides.", id);
        let discounts =  discounts_for_product_variants.remove(id).ok_or(Error::new(message))?;
        Ok((*id, discounts))
    }).collect()
}

/// Converts the GraphQL client library generated discounts to the internally used discounts, which are GraphQL `SimpleObject`.
///
/// This enables the discounts to be retrivable from the GraphQL endpoints of this service.
fn convert_graphql_client_lib_discounts_to_simple_object_discounts(
    graphql_client_lib_discounts: HashMap<Uuid, get_discounts::GetDiscountsFindApplicableDiscounts>,
) -> HashMap<Uuid, BTreeSet<Discount>> {
    graphql_client_lib_discounts
        .into_iter()
        .map(|(id, discounts)| {
            let discounts = discounts
                .discounts
                .into_iter()
                .map(
                    |discount: get_discounts::GetDiscountsFindApplicableDiscountsDiscounts| {
                        Discount::from(discount)
                    },
                )
                .collect();
            (id, discounts)
        })
        .collect()
}

/// Calculates the total sum of the undiscounted order items. Does not include shipping costs.
///
/// This defines the semantic of the total amount that is passed to the Discount service, for figuring out which Discounts apply.
/// Do not confuse with `calculate_compensatable_order_amount`, which is the total compensatable amount that the buyer needs to pay.
///
/// Converts value to an `i64` as this is what the GraphQL client library expects.
fn calculate_order_amount(
    pproduct_variant_versions_by_product_variant_ids: &HashMap<Uuid, ProductVariantVersion>,
) -> i64 {
    let order_amount: u32 = pproduct_variant_versions_by_product_variant_ids
        .iter()
        .map(|(_, p)| p.price)
        .sum();
    i64::from(order_amount)
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schemas_repo/shipment.graphql",
    query_path = "queries/get_shipment_fees.graphql",
    response_derives = "Debug"
)]
/// GraphQL query generated by client library.
struct GetShipmentFees;

/// Queries shipment fees for product variant versions and counts.
#[instrument(skip(order_item_inputs_by_product_variant_ids, product_variant_versions_by_product_variant_ids, counts_by_product_variant_ids), fields(product_variant_count = product_variant_versions_by_product_variant_ids.len()))]
async fn query_shipment_fees(
    order_item_inputs_by_product_variant_ids: &HashMap<Uuid, OrderItemInput>,
    product_variant_versions_by_product_variant_ids: &HashMap<Uuid, ProductVariantVersion>,
    counts_by_product_variant_ids: &HashMap<Uuid, u64>,
) -> Result<u64> {
    let calculate_shipment_fees_input = build_calculate_shipment_fees_input(
        product_variant_versions_by_product_variant_ids,
        counts_by_product_variant_ids,
        order_item_inputs_by_product_variant_ids,
    )?;
    let variables = get_shipment_fees::Variables {
        calculate_shipment_fees_input,
    };

    let request_body = GetShipmentFees::build_query(variables);
    let client = reqwest::Client::new();

    let res = client
        .post("http://localhost:3500/v1.0/invoke/shipment/method/graphql")
        .json(&request_body)
        .send()
        .await?;
    let response_body: Response<get_shipment_fees::ResponseData> = res.json().await?;
    let message = "Response data of `query_shipment_fees` query is empty.";
    let response_data: get_shipment_fees::ResponseData =
        response_body.data.ok_or(Error::new(message))?;
    let shipment_fees = u64::try_from(response_data.calculate_shipment_fees)?;
    Ok(shipment_fees)
}

/// Builds the `get_shipment_fees::CalculateShipmentFeesInput` by using product variant versions, counts and shipment methods.
fn build_calculate_shipment_fees_input(
    product_variant_versions_by_product_variant_ids: &HashMap<Uuid, ProductVariantVersion>,
    counts_by_product_variant_ids: &HashMap<Uuid, u64>,
    order_item_inputs_by_product_variant_ids: &HashMap<Uuid, OrderItemInput>,
) -> Result<get_shipment_fees::CalculateShipmentFeesInput, Error> {
    let items =
        product_variant_versions_by_product_variant_ids
            .iter()
            .map(|(id, product_variant_version)| {
                let count_error = build_hash_map_error(counts_by_product_variant_ids, *id);
                let count = counts_by_product_variant_ids.get(id).ok_or(count_error)?;
                let order_item_input_error =
                    build_hash_map_error(order_item_inputs_by_product_variant_ids, *id);
                let shipment_method_id: Uuid = order_item_inputs_by_product_variant_ids
                    .get(id)
                    .ok_or(order_item_input_error)?
                    .shipment_method_id;
                let product_variant_version_with_quantity_and_shipment_method_input =
                    get_shipment_fees::ProductVariantVersionWithQuantityAndShipmentMethodInput {
                        product_variant_version_id: product_variant_version._id,
                        quantity: i64::try_from(*count)?,
                        shipment_method_id,
                    };
                Ok(product_variant_version_with_quantity_and_shipment_method_input)
            })
            .collect::<Result<
                Vec<get_shipment_fees::ProductVariantVersionWithQuantityAndShipmentMethodInput>,
            >>()?;
    let calculate_shipment_fees_input = get_shipment_fees::CalculateShipmentFeesInput { items };
    Ok(calculate_shipment_fees_input)
}

/// Sends an `order/order/created` created event containing the order context.
#[instrument(skip(order_dto))]
async fn send_order_created_event(order_dto: OrderDTO) -> Result<()> {
    let client = reqwest::Client::new();
    client
        .post("http://localhost:3500/v1.0/publish/pubsub/order/order/created")
        .json(&order_dto)
        .send()
        .await?;
    Ok(())
}

/// Checks if an address is registered under a specific user (MongoDB database populated with events).
///
/// Used before creating orders.
#[instrument(skip(collection), fields(user_id = %user_id, address_id = %id))]
async fn validate_user_address(
    collection: &Collection<User>,
    id: Uuid,
    user_id: Uuid,
) -> Result<()> {
    match collection.find_one(doc! {"_id": user_id }, None).await {
        Ok(maybe_object) => match maybe_object {
            Some(_) => Ok(()),
            None => {
                let message = format!(
                    "User address with UUID: `{}` of user with UUID: `{}` not found.",
                    id, user_id
                );
                Err(Error::new(message))
            }
        },
        Err(_) => {
            let message = format!(
                "User address with UUID: `{}` of user with UUID: `{}` not found.",
                id, user_id
            );
            Err(Error::new(message))
        }
    }
}

/// Checks if a single object is in the system (MongoDB database populated with events).
///
/// Used before creating orders.
#[instrument(skip(collection), fields(id = %id))]
pub async fn validate_object<T: for<'a> Deserialize<'a> + Unpin + Send + Sync>(
    collection: &Collection<T>,
    id: Uuid,
) -> Result<()> {
    //db ping for debugging
    let start = Instant::now();
    let _ = collection.client().database("order-database").run_command(doc! { "ping": 1 }, None).await;
    println!("PING TIME: {:?}", start.elapsed());

    let start = Instant::now();
    let result = query_object(collection, id).await.map(|_| ());
    let duration = start.elapsed();
    println!("mongo real time: {:?}", duration);
    result
}

/// Checks if all objects are in the system (MongoDB database populated with events).
///
/// Used before creating orders.
#[instrument(skip(collection, object_ids))]
async fn validate_objects<T: for<'b> Deserialize<'b> + Unpin + Send + Sync + PartialEq + Clone>(
    collection: &Collection<T>,
    object_ids: Vec<Uuid>,
) -> Result<()>
where
    Uuid: From<T>,
{
    match collection
        .find(doc! {"_id": { "$in": &object_ids } }, None)
        .await
    {
        Ok(cursor) => {
            let objects: Vec<T> = cursor.try_collect().await?;
            let ids: Vec<Uuid> = objects
                .iter()
                .map(|object: &T| Uuid::from(object.clone()))
                .collect();
            object_ids
                .iter()
                .fold(Ok(()), |o, id| match ids.contains(id) {
                    true => o.and(Ok(())),
                    false => {
                        let message = format!(
                            "{} with UUID: `{}` is not present in the system.",
                            type_name::<T>(),
                            id
                        );
                        Err(Error::new(message))
                    }
                })
        }
        Err(_) => {
            let message = format!(
                "{} with specified UUIDs are not present in the system.",
                type_name::<T>()
            );
            Err(Error::new(message))
        }
    }
}

/// Returns an error of a hash map retrieval.
///
/// Constructs error message that describes a failed retrieval of an item `V` by product variant UUID.
fn build_hash_map_error<V>(_hash_map: &HashMap<Uuid, V>, id: Uuid) -> Error {
    let message = format!(
        "`{}` for product variant of UUID: `{}` is not present in `{}`. ",
        type_name::<V>(),
        id,
        type_name::<HashMap<Uuid, V>>()
    );
    Error::new(message)
}
