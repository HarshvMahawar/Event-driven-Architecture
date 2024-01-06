import json
import logging
import time
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
from confluent_kafka import Producer, KafkaException
from django.conf import settings

# Configure logging
logging.basicConfig(filename='/app/logs/producer_logs.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
producer = Producer({"bootstrap.servers": settings.KAFKA_BROKER})

def serialize_user_data(user_data):
    return json.dumps(user_data)

@csrf_exempt
def produce(request):
    # Validate request method
    if request.method != 'POST':
        return HttpResponseBadRequest("Invalid request method. Only POST is allowed.")

    try:
        # Extract and process user data
        data = json.loads(request.body)
        name = data.get('name')
        answer_1 = data.get('answer_1')
        answer_2 = data.get('answer_2')
        answer_3 = data.get('answer_3')

        user_data = {'name': name, 'answer_1': answer_1, 'answer_2': answer_2, 'answer_3': answer_3}

        # Record start time
        start_time = time.time()
        
        # Produce Kafka event
        serialized_data = serialize_user_data(user_data)
        producer.produce(settings.GOOGLESHEETSINTEGRATION_EVENTS_TOPIC, key="data", value=serialized_data)
        producer.flush()
        producer.poll(1)
        
        # Record end time and calculate elapsed time
        end_time = time.time()
        elapsed_time = end_time - start_time

        return JsonResponse({"Status": "Success", "user_data": user_data, "elapsed_time": elapsed_time})
    except KafkaException as ke:
        # Log Kafka-specific exceptions
        logger.error(f"[{time.ctime()}] Kafka Error: {str(ke)}")
        return HttpResponseBadRequest(f"Kafka Error: {str(ke)}")
    except Exception as e:
        # Log other general exceptions
        logger.error(f"[{time.ctime()}] Error: {str(e)}")
        return HttpResponseBadRequest(f"Error: {str(e)}")

