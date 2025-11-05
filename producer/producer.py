import os
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from logger import get_logger
import pika
from bson import json_util

logger = get_logger()

def get_database():
    uri = F"mongodb+srv://{os.environ.get('DB_USER')}_db_user:{os.environ.get('DB_PASSWORD')}@nestjs-challenge.ora4ujs.mongodb.net/?appName=nestjs-challenge"
    client = MongoClient(uri, server_api=ServerApi('1'))

    try:
        client.admin.command('ping')
        logger.info(f"Connected to MongoDB: {os.environ.get('DB_NAME')}.videoLogs")
        logger.info("Watching for changes...")
    except Exception as e:
        logger.error(e)

    return client



def producer():
    client = get_database()
    db = client["etlChallenge"]
    collection = db['videoLogs']

    try:
        with collection.watch(full_document="updateLookup") as stream:
            for change in stream:
                logger.info(f"Change detected: operation {change['operationType']}")
                publish_to_queue(change)

    except PyMongoError as e:
        logger.error(f"MongoDB error: {e}")
    except KeyboardInterrupt:
        logger.error("\nStopping change stream...")
    finally:
        client.close()
        logger.info("Connection closed")



def publish_to_queue(change):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(os.environ.get('RABBITMQ_HOST'))
    )

    logger.info(f"   ---- Connecting to: {os.environ.get('RABBITMQ_HOST')} ---")
    channel = connection.channel()
    channel.queue_declare(queue='video_log', durable=True)

    message = {
        "operation": change['operationType'],
        "document_id": str(change['documentKey']['_id']),
        "timestamp": str(change['clusterTime']),
        "data": change.get('fullDocument')
    }

    channel.confirm_delivery()
    channel.basic_publish(
        exchange='',
        routing_key='video_log',
        body=json_util.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # persistent
    )

    channel.close()
    connection.close()


def main():
    producer()


if __name__ == '__main__':
    load_dotenv()
    main()