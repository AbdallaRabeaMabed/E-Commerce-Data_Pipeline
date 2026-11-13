from pathlib import Path

from config import spark

base_path = Path('/app/datasets')
if not base_path.exists():
    base_path = Path('datasets')

tables = {
    'customers': 'customers.csv',
    'geolocation': 'geolocation.csv',
    'orders': 'orders.csv',
    'order_items': 'order_items.csv',
    'payments': 'payments.csv',
    'products': 'products.csv',
    'product_category_name_translation': 'product_category_name_translation.csv',
    'reviews': 'reviews.csv',
    'sellers': 'sellers.csv',
}

for name, file in tables.items():
    print(f'✅ {name} start')
    df = (
        spark.read
        .option('header', True)
        .option('inferschema', True)
        .csv(str(base_path / file))
    )
    df.write.mode('overwrite').parquet(f'Ingestion/bronze/{name}')
    print(f'✅ {name} end')

spark.stop()
print(f'🔴 spark Stopped')
