from dagster import Definitions
from dagster_pipeline1.jobs import stock_data_pipeline

from .ops import fetch_stock_data, save_data_to_minio

defs = Definitions(
    jobs=[stock_data_pipeline]
)
