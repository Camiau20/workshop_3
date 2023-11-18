# WORKSHOP 3: MACHINE LEARNING AND DATA STREAMING
By Camila Andrea Cardona Alzate

## Workshop Overview:

This workshop aims to make a prediction of the happiness score, making use of five csv files of rankings from various countries, from 2015 to 2019. In this case, we will make use of an exploratory data analysis, the respective transformations to concatenate the five files, stream the data using Kafka, and evaluate the prediction accuracy of the model used. 

## Tools used
- Pandas, to manipulate the data.
- Postgres to create the table of happiness index, and insert data with the final prediction.
- Docker to run Kafka and create a topic.
- Kafka to stream the data and make predictions.

## Setup:

1. Clone this repository:
   
    ```python
    git clone https://github.com/Camiau20/workshop_3.git)https://github.com/Camiau20/workshop_3.git
    ```
2. Create a virtual environment and activate it:
   `python -m venv env`

3. Install the dependencies:
   
   ```python
    pip install -r requirements.txt
    ```
4. Go to the docker folder and run the docker compose:

    ```bash
    docker-compose up -d
    ```

5. Enter to the Kafka bash and create a new topic with:
    
    ```bash
    docker exec -it kafka-test bash
    kafka-topics --bootstrap-server kafka-test:9092 --create --topic topic_name
    ```
**NOTE:** The topic_name you choose must be change on the [producer_features.py](services/producer_features.py) and the [consumer_predictions.py](services/consumer_predictions.py).

6. Create database credentials file 'config_db.json' with the following structure and fill it with your own credentials:
    ```
    {
      "user":"",
      "password":"",
      "host":"",
      "server": "",
      "db":""
    }
    ```
7. After you have your data in the DB you can run the [metrics.py](metrics.py) to get the metrics of the model precision. 
