from config import spark
from pyspark.sql.functions import col, to_timestamp


customers_bronze = spark.read.parquet('Ingestion/bronze/customers')
geolocation_bronze = spark.read.parquet('Ingestion/bronze/geolocation')
orders_bronze = spark.read.parquet('Ingestion/bronze/orders')
order_items_bronze = spark.read.parquet('Ingestion/bronze/order_items')
payments_bronze = spark.read.parquet('Ingestion/bronze/payments')
products_bronze = spark.read.parquet('Ingestion/bronze/products')
translation_bronze = spark.read.parquet(
    'Ingestion/bronze/product_category_name_translation')
reviews_bronze = spark.read.parquet('Ingestion/bronze/reviews')
sellers_bronze = spark.read.parquet('Ingestion/bronze/sellers')


print('✅ Customers Started')
customer_silver = (
    customers_bronze.dropDuplicates(['customer_id'])
    .filter(col('customer_unique_id').isNotNull())
)
customer_silver.write.mode('overwrite').parquet('Ingestion/silver/customers')

print('✅ Customers End')


print('✅ Geolocation Started')
geolocation_silver = (
    geolocation_bronze
    .dropDuplicates()
    .filter(col('geolocation_zip_code_prefix').isNotNull())
)
geolocation_silver.write.mode('overwrite').parquet(
    'Ingestion/silver/geolocation')
print('✅ Geolocation End')


print('✅ Orders Started')
order_silver = (
    orders_bronze.dropDuplicates(['order_id'])
    .filter(col('customer_id').isNotNull())
)

dates = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for date in dates:
    order_silver = order_silver.withColumn(date, to_timestamp(date))

order_silver.write.mode('overwrite').parquet('Ingestion/silver/orders')

print('✅ Orders End')

print('✅ Order Items Start')
order_item_silver = (
    order_items_bronze.dropDuplicates(['order_id', 'order_item_id'])
    .filter(
        col('order_item_id').isNotNull() & col(
            'product_id').isNotNull() & col('seller_id').isNotNull()
    )
    .withColumn('shipping_limit_date', to_timestamp('shipping_limit_date'))
)
order_item_silver.write.mode('overwrite').parquet(
    'Ingestion/silver/order_items')
print('✅ Order Items End')


print('✅ Payments Started')
payment_silver = (
    payments_bronze
    .dropDuplicates(['order_id', 'payment_sequential'])
    .filter(
        col('order_id').isNotNull()
        & col('payment_type').isNotNull()
    )
)
payment_silver.write.mode('overwrite').parquet('Ingestion/silver/payments')
print('✅ Payments End')


print('✅ Products Started')
product_silver = (
    products_bronze
    .dropDuplicates(['product_id'])
    .filter(col('product_id').isNotNull())
)
product_silver.write.mode('overwrite').parquet('Ingestion/silver/products')
print('✅ Products End')


print('✅ Product Category Translation Started')
translation_silver = (
    translation_bronze
    .dropDuplicates(['product_category_name'])
    .filter(
        col('product_category_name').isNotNull()
        & col('product_category_name_english').isNotNull()
    )
)
translation_silver.write.mode('overwrite').parquet(
    'Ingestion/silver/product_category_name_translation')
print('✅ Product Category Translation End')


print('✅ Reviews Started')
review_silver = (
    reviews_bronze
    .dropDuplicates(['review_id'])
    .filter(
        col('review_id').isNotNull()
        & col('order_id').isNotNull()
    )
)

review_dates = ['review_creation_date', 'review_answer_timestamp']
for date in review_dates:
    review_silver = review_silver.withColumn(date, to_timestamp(date))

review_silver.write.mode('overwrite').parquet('Ingestion/silver/reviews')
print('✅ Reviews End')


print('✅ Sellers Started')
seller_silver = (
    sellers_bronze
    .dropDuplicates(['seller_id'])
    .filter(col('seller_id').isNotNull())
)
seller_silver.write.mode('overwrite').parquet('Ingestion/silver/sellers')
print('✅ Sellers End')


spark.stop()
print('🔴 spark Stopped')
