from fastapi import FastAPI, HTTPException, status
from google.cloud import pubsub_v1
import os
import json
import logging
from .models import TransactionEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingestion-api")

app = FastAPI(title="Financial Risk Ingestion API", version="1.0.0")

# GCP settings
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "financial-data-platform-508216")
TOPIC_ID = os.getenv("PUBSUB_TOPIC", "transaction-events")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@app.get("/health")
def health_check():
    """Basic health check endpoint"""
    return {"status": "healthy"}

@app.post("/ingest/transaction", status_code=status.HTTP_202_ACCEPTED)
async def ingest_transaction(transaction: TransactionEvent):
    """
    Ingest a transaction event, validate it using Pydantic, 
    and publish it to Google Cloud Pub/Sub.
    """
    try:
        # Convert timestamp to string for JSON serialization
        txn_dict = transaction.model_dump()
        txn_dict['timestamp'] = txn_dict['timestamp'].isoformat()
        
        message_json = json.dumps(txn_dict).encode("utf-8")
        
        # Publish to Pub/Sub
        future = publisher.publish(topic_path, message_json)
        message_id = future.result()
        
        logger.info(f"Published transaction {transaction.transaction_id} to Pub/Sub with message ID {message_id}")
        
        return {
            "status": "success",
            "message": "Transaction published successfully",
            "transaction_id": transaction.transaction_id,
            "message_id": message_id
        }
    except Exception as e:
        logger.error(f"Error publishing to Pub/Sub: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish transaction: {str(e)}"
        )
