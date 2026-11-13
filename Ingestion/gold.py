from config import spark
from pyspark.sql.functions import coalesce, col, count, lit, sum as spark_sum


jdbc_url = "jdbc:postgresql://postgres:5432/warehouse"
properties = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver"
}


def load_table(dataframe, table_name, label):
    dataframe.write.mode('overwrite').jdbc(
        jdbc_url,
        table=table_name,
        properties=properties
    )
    print(f'✅ Load {label} On DataWareHouse')


customers_silver = spark.read.parquet('Ingestion/silver/customers')
geolocation_silver = spark.read.parquet('Ingestion/silver/geolocation')
orders_silver = spark.read.parquet('Ingestion/silver/orders')
order_items_silver = spark.read.parquet('Ingestion/silver/order_items')
payments_silver = spark.read.parquet('Ingestion/silver/payments')
products_silver = spark.read.parquet('Ingestion/silver/products')
translation_silver = spark.read.parquet(
    'Ingestion/silver/product_category_name_translation')
reviews_silver = spark.read.parquet('Ingestion/silver/reviews')
sellers_silver = spark.read.parquet('Ingestion/silver/sellers')


fact_order_items = order_items_silver.select(
    'order_id',
    'order_item_id',
    'product_id',
    'seller_id',
    'shipping_limit_date',
    'price',
    'freight_value'
)
load_table(fact_order_items, 'fact_order_items', 'Fact Order Items')


order_item_metrics = (
    order_items_silver
    .groupBy('order_id')
    .agg(
        spark_sum('price').alias('total_revenue'),
        spark_sum('freight_value').alias('total_freight_value'),
        count('order_item_id').alias('total_order_items')
    )
)

payment_metrics = (
    payments_silver
    .groupBy('order_id')
    .agg(
        spark_sum('payment_value').alias('total_payment_value'),
        count('payment_sequential').alias('total_payments')
    )
)

fact_orders = (
    orders_silver
    .join(order_item_metrics, 'order_id', 'left')
    .join(payment_metrics, 'order_id', 'left')
    .select(
        orders_silver.order_id,
        orders_silver.customer_id,
        orders_silver.order_status,
        orders_silver.order_purchase_timestamp,
        orders_silver.order_approved_at,
        orders_silver.order_delivered_carrier_date,
        orders_silver.order_delivered_customer_date,
        orders_silver.order_estimated_delivery_date,
        coalesce(col('total_revenue'), lit(0)).alias('total_revenue'),
        coalesce(col('total_freight_value'), lit(0)).alias(
            'total_freight_value'),
        coalesce(col('total_order_items'), lit(0)).alias('total_order_items'),
        coalesce(col('total_payment_value'), lit(0)).alias(
            'total_payment_value'),
        coalesce(col('total_payments'), lit(0)).alias('total_payments')
    )
)
load_table(fact_orders, 'fact_orders', 'Fact Orders')


fact_payments = payments_silver.select(
    'order_id',
    'payment_sequential',
    'payment_type',
    'payment_installments',
    'payment_value'
)
load_table(fact_payments, 'fact_payments', 'Fact Payments')


fact_reviews = reviews_silver.select(
    'review_id',
    'order_id',
    'review_score',
    'review_comment_title',
    'review_comment_message',
    'review_creation_date',
    'review_answer_timestamp'
)
load_table(fact_reviews, 'fact_reviews', 'Fact Reviews')


dim_customers = customers_silver.select(
    'customer_id',
    'customer_unique_id',
    'customer_zip_code_prefix',
    'customer_city',
    'customer_state'
)
load_table(dim_customers, 'customers', 'Customers')


dim_geolocation = geolocation_silver.select(
    'geolocation_zip_code_prefix',
    'geolocation_lat',
    'geolocation_lng',
    'geolocation_city',
    'geolocation_state'
)
load_table(dim_geolocation, 'geolocation', 'Geolocation')


dim_products = products_silver.select(
    'product_id',
    'product_category_name',
    'product_name_lenght',
    'product_description_lenght',
    'product_photos_qty',
    'product_weight_g',
    'product_length_cm',
    'product_height_cm',
    'product_width_cm'
)
load_table(dim_products, 'products', 'Products')


dim_sellers = sellers_silver.select(
    'seller_id',
    'seller_zip_code_prefix',
    'seller_city',
    'seller_state'
)
load_table(dim_sellers, 'sellers', 'Sellers')


dim_translation = translation_silver.select(
    'product_category_name',
    'product_category_name_english'
)
load_table(
    dim_translation,
    'product_category_name_translation',
    'Product Category Translation'
)


spark.stop()
print('🔴 spark Stopped')
