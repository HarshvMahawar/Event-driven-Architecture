import json
import logging
import time
from django.conf import settings
from .gsheet_client import get_spreadsheet_client
from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import threading

# Initialize logging
log_file_path = '/app/logs/googlesheets/consumer_logs.log'
logging.basicConfig(filename=log_file_path)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 

class GoogleSheetConsumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.consumer = Consumer({
            'bootstrap.servers': settings.KAFKA_BROKER,
            'group.id': settings.GOOGLESHEETSINTEGRATION_EVENTS_TOPIC + "-CONSUMER",
            'auto.offset.reset': 'earliest'
        })
        self.batch_size = 10
        self.records_buffer = []
        self.spreadsheet_client = get_spreadsheet_client()

    def save_records_to_google_sheets(self, records):
        try:
            config = self.read_config()
            sheet_id = config.get('googleSheet', {}).get('sheetId')
            sheet_name = config.get('googleSheet', {}).get('sheetName')
            range_ = f"{sheet_name}!A:E"

            values = []
            for record in records:
                values.append([record['name'], record['answer_1'], record['answer_2'], record['answer_3']])

            body = {'values': values}
            result = self.spreadsheet_client.values().append(
                spreadsheetId=sheet_id,
                range=range_,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            print("Records saved!")
            logger.info(f"Records saved to Google Sheets: {result}")
        except Exception as e:
            logger.error(f"Error saving records to Google Sheets: {e}")

    def read_config(self):
        with open("/app/config_files/config.json", "r") as config_file:
            return json.load(config_file)

    def run(self):
        """
        Main execution loop for the Kafka consumer.
        """
        start_time = time.time()  # Record the start time
        print("Started GoogleSheetIntegration Listener")
        logger.info('Started GoogleSheetIntegration Listener at %s', time.ctime(start_time))
        try:
            self.consumer.subscribe([settings.GOOGLESHEETSINTEGRATION_EVENTS_TOPIC])
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
                else:
                    received_time = time.time()
                    print("Recieved")
                    logger.info('Received message at %s', time.ctime(received_time))
                    try:
                        message = json.loads(msg.value().decode('utf-8'))
                        self.records_buffer.append(message)

                        if len(self.records_buffer) >= self.batch_size:
                            self.save_records_to_google_sheets(self.records_buffer)
                            self.records_buffer = []

                    except json.JSONDecodeError as json_error:
                        logger.error(f"Error decoding JSON: {json_error}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

        finally:
            if self.records_buffer:
                self.save_records_to_google_sheets(self.records_buffer)
                self.records_buffer = []

            self.consumer.close()

