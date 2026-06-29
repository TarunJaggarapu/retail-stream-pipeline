import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import WorkerOptions
import json
import logging
from datetime import datetime

PROJECT_ID = "retail-stream-pipeline"
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/retail-orders-sub"
DATASET = "retail_analytics"
TABLE_ORDERS = f"{PROJECT_ID}:{DATASET}.orders"
TABLE_ORDER_ITEMS = f"{PROJECT_ID}:{DATASET}.order_items"

# BigQuery schemas
ORDERS_SCHEMA = {
    "fields": [
        {"name": "order_id", "type": "STRING"},
        {"name": "user_id", "type": "INTEGER"},
        {"name": "user_name", "type": "STRING"},
        {"name": "user_email", "type": "STRING"},
        {"name": "user_city", "type": "STRING"},
        {"name": "order_date", "type": "STRING"},
        {"name": "total_amount_usd", "type": "FLOAT"},
        {"name": "total_amount_inr", "type": "FLOAT"},
        {"name": "currency_rate_usd_inr", "type": "FLOAT"},
        {"name": "ingested_at", "type": "STRING"},
        {"name": "processed_at", "type": "STRING"},
    ]
}

ORDER_ITEMS_SCHEMA = {
    "fields": [
        {"name": "order_id", "type": "STRING"},
        {"name": "product_id", "type": "INTEGER"},
        {"name": "product_title", "type": "STRING"},
        {"name": "category", "type": "STRING"},
        {"name": "quantity", "type": "INTEGER"},
        {"name": "unit_price_usd", "type": "FLOAT"},
        {"name": "total_price_usd", "type": "FLOAT"},
    ]
}


class ParseOrderFn(beam.DoFn):
    def process(self, message):
        try:
            event = json.loads(message.decode("utf-8"))
            event["processed_at"] = datetime.utcnow().isoformat()
            yield event
        except Exception as e:
            logging.error(f"Failed to parse message: {e}")


class ExtractOrderRowFn(beam.DoFn):
    def process(self, event):
        yield {
            "order_id": event["order_id"],
            "user_id": event["user_id"],
            "user_name": event["user_name"],
            "user_email": event["user_email"],
            "user_city": event["user_city"],
            "order_date": event["order_date"],
            "total_amount_usd": event["total_amount_usd"],
            "total_amount_inr": event["total_amount_inr"],
            "currency_rate_usd_inr": event["currency_rate_usd_inr"],
            "ingested_at": event["ingested_at"],
            "processed_at": event["processed_at"],
        }


class ExtractOrderItemsFn(beam.DoFn):
    def process(self, event):
        for item in event.get("items", []):
            yield {
                "order_id": event["order_id"],
                "product_id": item["product_id"],
                "product_title": item["product_title"],
                "category": item["category"],
                "quantity": item["quantity"],
                "unit_price_usd": item["unit_price_usd"],
                "total_price_usd": item["total_price_usd"],
            }


def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = "DataflowRunner"

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.region = "us-west1"
    gcp_options.staging_location = "gs://retail-stream-pipeline-bucket-east/staging"
    gcp_options.temp_location = "gs://retail-stream-pipeline-bucket-east/temp"
    gcp_options.job_name = "retail-stream-job-3"

    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type = "e2-medium"
    worker_options.disk_size_gb = 30
    worker_options.num_workers = 1
    worker_options.max_num_workers = 1

    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(subscription=SUBSCRIPTION)
        )

        parsed = (
            messages
            | "ParseOrders" >> beam.ParDo(ParseOrderFn())
        )

        # Write orders table
        (
            parsed
            | "ExtractOrderRows" >> beam.ParDo(ExtractOrderRowFn())
            | "WriteOrders" >> WriteToBigQuery(
                TABLE_ORDERS,
                schema=ORDERS_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # Write order items table
        (
            parsed
            | "ExtractOrderItems" >> beam.ParDo(ExtractOrderItemsFn())
            | "WriteOrderItems" >> WriteToBigQuery(
                TABLE_ORDER_ITEMS,
                schema=ORDER_ITEMS_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()