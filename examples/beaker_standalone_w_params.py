import os, sys
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from dotenv import load_dotenv
load_dotenv()

sys.path.append("../src")

from beaker import benchmark, sqlwarehouseutils

hostname = os.getenv("DATABRICKS_HOST")
# Don't put tokens in plaintext in code
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
catalog_name = os.getenv("CATALOG")
schema_name = os.getenv("SCHEMA")

print("---- USE EXISTING WAREHOUSE ------")

http_path = os.getenv("DATABRICKS_HTTP_PATH")

bm = benchmark.Benchmark()
bm.setName(name="simple_test_w_params")
bm.setHostname(hostname=hostname)
bm.setWarehouseToken(token=access_token)
bm.setWarehouse(http_path=http_path)
bm.setQueryRepeatCount(1)
bm.setConcurrency(2)
bm.setCatalog(catalog=catalog_name)
bm.setSchema(schema=schema_name)


print("---- Specify a single query file with params ------")

bm.setQueryFile("queries_params/q1.sql")
bm.setParamsPath("queries_params/params.json")
metrics_pdf = bm.execute()
print(metrics_pdf)

print("---- Specify a query directory with params ------")
bm.setQueryFileDir("queries_params")
bm.setParamsPath("queries_params/params.json")
metrics_pdf = bm.execute()
print(metrics_pdf)
