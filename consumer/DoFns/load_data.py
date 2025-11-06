from datetime import datetime
import apache_beam as beam
import clickhouse_connect
from logger import get_logger

logger = get_logger()

class WriteToDataWarehouse(beam.DoFn):
    def __init__(self, operation_type):
        self.operation_type = operation_type


    def setup(self):
        logger.info("-------- Setting up clickhouse connection ----------")
        self.client = clickhouse_connect.get_client(host='clickhouse', username='default', password='clickhouse-password')
        logger.info("-------- Connected to clickhouse :) ----------")


    def process(self, element):
        try:
            # Organize data for warehouse client functions
            if self.operation_type == 'insert' or self.operation_type == 'update':
                self.writeRecord(element)
            else:
                logger.warning(f"Unknown operation: {self.operation_type}")

            yield element

        except Exception as e:
            logger.error(f"Error writing to warehouse: {e}")


    def writeRecord(self, element):
        try:
            timestamp = element.get('timestamp')
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')

            data = [[
                element.get('document_id'),
                timestamp,
                element.get('video_id'),
                element.get('session_id'),
                element.get('watched_seconds'),
                element.get('video_duration_seconds'),
                element.get('watched_ratio'),
                element.get('device_type'),
                element.get('quality'),
                False  # is_deleted: False for active records
            ]]

            self.client.insert(
                'video_analytics.video_logs',
                data,
                column_names=[
                    'original_id', 'original_timestamp', 'video_id',
                    'session_id', 'watched_seconds', 'video_duration_seconds',
                    'watched_ratio', 'device_type', 'quality', 'is_deleted'
                ]
            )

            logger.info(f"✓ Wrote record: {element.get('document_id')}")

        except Exception as e:
            logger.error(f"Error writing record: {e}")
            raise

    def deleteRecord(self, element):
        # Should have logic if a record should be softly deleted from the warehouse
        return
        # Example of what it should look like
        # try:
        #     # Insert a new version marking the record as deleted
        #     data = [[
        #         element.get('document_id'),
        #         element.get('timestamp'),
        #         element.get('video_id', ''),
        #         element.get('session_id', ''),
        #         element.get('watched_seconds'),
        #         element.get('video_duration_seconds'),
        #         element.get('watched_ratio'),
        #         element.get('device_type', ''),
        #         element.get('quality', ''),
        #         True  # is_deleted: True for deleted records
        #     ]]
        #
        #     self.client.insert(
        #         'video_analytics.video_logs',
        #         data,
        #         column_names=[
        #             'original_id', 'original_timestamp', 'video_id',
        #             'session_id', 'watched_seconds', 'video_duration_seconds',
        #             'watched_ratio', 'device_type', 'quality', 'is_deleted'
        #         ]
        #     )
        #
        #     logger.info(f"✓ Soft deleted record: {element.get('document_id')}")
        #
        # except Exception as e:
        #     logger.error(f"Error deleting record: {e}")
        #     raise
