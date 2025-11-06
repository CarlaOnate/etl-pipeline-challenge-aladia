import json
import os
import apache_beam as beam
from DoFns.calculate_watched_ratio import CalculateWatchedRatio
from DoFns.modify_structure import ModifyStructure
from DoFns.load_data import WriteToDataWarehouse
from logger import get_logger
import pika

logger = get_logger()

def get_message():
    connection = pika.BlockingConnection(pika.ConnectionParameters(os.environ.get('RABBITMQ_HOST')))
    channel = connection.channel()
    channel.queue_declare(queue='video_log', durable=True)
    channel.basic_consume(queue='video_log', on_message_callback=callback, auto_ack=False)

    logger.info(' [*] Waiting for messages')
    channel.start_consuming()


def callback(ch, method, properties, body):
    logger.info(f"\n[*] Received message")

    try:
        message_str = body.decode('utf-8')
        message_data = json.loads(message_str)
        operation_type = message_data['operation']

        # Create and run pipeline for this single message
        with beam.Pipeline() as pipeline:
            (
                    pipeline
                    | 'CreateInput' >> beam.Create([message_data])
                    | 'CalculateWatchedRatio' >> beam.ParDo(CalculateWatchedRatio())
                    | 'ModifyStructure' >> beam.ParDo(ModifyStructure())
                    | 'WriteToDataWarehouse' >> beam.ParDo(WriteToDataWarehouse(operation_type))
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("[✓] Message processed successfully\n")
    except Exception as e:
        print(f"[✗] Error processing message: {e}\n")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def consumer():
    try:
        get_message()
    except Exception as e:
        logger.error(f"Error: {e}")
    except KeyboardInterrupt:
        logger.error("\nStopping consumer...")


def main():
    consumer()


if __name__ == '__main__':
    main()