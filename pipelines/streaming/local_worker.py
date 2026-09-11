import json
import logging
import sys
from concurrent.futures import TimeoutError
from pathlib import Path

from google.cloud import bigquery
from google.cloud import pubsub_v1

# Add project root to sys.path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root))

from pipelines.streaming.transforms import (  # noqa: E402
    EnrichTransaction,
    FormatForBigQuery,
    ScoreFraudRisk,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("local_worker")

project_id = "financial-data-platform-508216"
subscription_id = "transaction-events-sub"
dataset_id = "analytics"
table_id = "transaction_risk"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
bq_client = bigquery.Client(project=project_id)
table_ref = bq_client.dataset(dataset_id).table(table_id)

enricher = EnrichTransaction()
scorer = ScoreFraudRisk()
scorer.setup() # for sys.path
formatter = FormatForBigQuery()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data = message.data.decode("utf-8")
        record = json.loads(data)
        
        # 1. Enrich
        enriched_list = list(enricher.process(record))
        if not enriched_list:
            message.ack()
            return
        enriched = enriched_list[0]
        
        # 2. Score
        scored_list = list(scorer.process(enriched))
        if not scored_list:
            message.ack()
            return
            
        scored = scored_list[0]
        if isinstance(scored, type(sys.modules['apache_beam.pvalue'].TaggedOutput)):
            logger.error(f"Scoring error: {scored.value}")
            message.ack()
            return
            
        # 3. Format
        final_list = list(formatter.process(scored))
        if not final_list:
            message.ack()
            return
        final_record = final_list[0]
        
        # 4. Insert to BQ
        errors = bq_client.insert_rows_json(table_ref, [final_record])
        if errors:
            logger.error(f"Failed to insert row: {errors}")
        else:
            logger.info(f"Successfully inserted transaction {final_record.get('transaction_id')}")
            
        message.ack()
    except Exception as e:
        logger.error(f"Exception processing message: {e}")
        message.ack() # ACK anyway to drain

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
logger.info(f"Listening for messages on {subscription_path}..\n")

try:
    # Run for 60 seconds to populate the dashboard, then exit
    streaming_pull_future.result(timeout=60)
except TimeoutError:
    streaming_pull_future.cancel()
    streaming_pull_future.result()
