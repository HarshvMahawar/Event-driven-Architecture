## What this project is about?
This self-project built a real-time data platform using Apache Kafka and Django. Highlights include:
- Scalable data processing: Kafka handles millions of events efficiently via horizontal scaling.
- Modular architecture: Plug-in-play design allows for easy integration of new functionalities.
- Robust monitoring: Error handling and logging ensure smooth operation and valuable insights.
  
This project demonstrates expertise in event-driven architecture, containerization, and real-time data processing.
## Installation guide
- You will need to have docker, docker-compose and Python installed
### Commands
- Start Zookeper Container and expose PORT `2181`.
```bash
docker run -p 2181:2181 zookeeper
```
- Start Kafka Container, expose PORT `9092` and setup ENV variables.
```bash
docker run -p 9092:9092 \
-e KAFKA_ZOOKEEPER_CONNECT=<PRIVATE_IP>:2181 \
-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://<PRIVATE_IP>:9092 \
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
confluentinc/cp-kafka
```
- Clone or download this repo and inside the repo modify the settings.py file to add your IP Address in Kafka configuration at this path
```bash
cd path/to/repo/DataPlatform/DataPlatform/settings.py
```
- kafka Configuration
<img width="437" alt="kafka" src="https://github.com/HarshvMahawar/RefinedApproach/assets/114311884/8d53d47e-0c18-4afd-821a-3d2bdad7f5b7">

- Now run docker compose, docker image of the current project will build and Postgres (Database used) docker image will be fetched
```bash
cd path/to/repo/DataPlatform
docker-compose up
```
- Run this command to start the bash inside the DataPlatform docker container
```bash
docker exec -it <dataplatform-container-id> bash
```
- The Django server is up, you can test the endpoints using POSTMAN or any other API testers
  - For BusinessRule endpoint, start the Queue listener (Kafka Consumer) using following command(inside the container)
    ```bash
    python manage.py launch_br_queue_listener
    ```
  - Now, all the messages/requests POSTED to the endpoint will be recieved and processed by the consumer.
  - You can check the logs in the `/app/logs` directory inside the docker container.
- For using the GoogleSheetsIntegration endpoint you will need to download the credentials.json and create the token.json files following this [guide](https://developers.google.com/sheets/api/quickstart/python)
- Save the credentials.json and token.json in the Django project directory and modify the `config_files/config.json` to add you Google Sheet details where the data will be stored.
- Start the googlesheetsintegration queue listener (Kafka Consumer) using following command (inside the container)
```bash
python manage.py launch_gsheet_queue_listener
```
- Thats all, data POSTED on the particular endpoint will be stored in the google sheet, check the logs if needed.
- Thank You
