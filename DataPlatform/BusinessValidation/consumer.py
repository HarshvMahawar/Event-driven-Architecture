import json
import logging
import time  # Added import for the time module
from django.conf import settings
from django.db import transaction, IntegrityError
from .models import BusinessValidationRecord
from confluent_kafka import Consumer, KafkaError, KafkaException
import threading
import sys

# Initialize logging
log_file_path = '/app/logs/businessrule/consumer_logs.log'
logging.basicConfig(filename=log_file_path, level=logging.INFO)
logger = logging.getLogger(__name__)

SALARY_THRESHOLD = 45000


class BusinessRuleListener(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.consumer = Consumer({
            'bootstrap.servers': settings.KAFKA_BROKER,
            'group.id': settings.BUSINESSVALIDATION_EVENTS_TOPIC + "-CONSUMER",
            'auto.offset.reset': 'earliest'
        })
        self.batch_size = 10
        self.records_buffer = []

    def save_records_to_database(self, records):
        """
        Save a batch of records to the database in an atomic transaction.
        """
        try:
            with transaction.atomic():
                BusinessValidationRecord.objects.bulk_create(records)
                print("Records saved!")
                logger.info("Records saved!")
        except IntegrityError as e:
            logger.error(f"Error saving records to the database: {e}")

    def run(self):
        """
        Main execution loop for the Kafka consumer.
        """
        start_time = time.time()  # Record the start time
        print("Started BusinessRule Listener")
        logger.info('Started BusinessRule Listener at %s', time.ctime(start_time))
        try:
            self.consumer.subscribe([settings.BUSINESSVALIDATION_EVENTS_TOPIC])
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event, not an error
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
                else:
                    received_time = time.time()  # Record the time of message reception
                    print("Received")
                    logger.info('Received message at %s', time.ctime(received_time))
                    try:
                        message = json.loads(msg.value().decode('utf-8'))

                        if 'salary' in message and message['salary'] < SALARY_THRESHOLD:
                            logger.warning('Flag raised! Salary below threshold.')
                        else:
                            record = BusinessValidationRecord(
                                name=message.get('name', ''),
                                salary=message.get('salary', 0.0),
                                saving=message.get('saving', 0.0)
                            )
                            self.records_buffer.append(record)

                            if len(self.records_buffer) >= self.batch_size:
                                self.save_records_to_database(self.records_buffer)
                                self.records_buffer = []

                    except json.JSONDecodeError as json_error:
                        logger.error(f"Error decoding JSON: {json_error}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

        finally:
            if self.records_buffer:
                self.save_records_to_database(self.records_buffer)
                self.records_buffer = []

            self.consumer.close()
