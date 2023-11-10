import json
import joblib
import pandas as pd
import psycopg2 as psy

from json import dumps, loads
from kafka import KafkaConsumer


def load_model():
    """Load the model from pkl file"""
    file = "../model/trained_model.pkl"
    model = joblib.load(file)
    return model

def db_connection():
    """Connect to the database"""
    with open('../secrets/config_db.json') as config_json:
        config = json.load(config_json)
    conn = psy.connect(**config) 
    return conn

def create_table():
    """Create table hapiness_index if not exists"""
    conn = db_connection()
    cursor= conn.cursor()

    create_table_query = """CREATE TABLE IF NOT EXISTS happiness_index (year INTEGER, country VARCHAR(300), gdp_per_capita NUMERIC, healthy_life_expectancy NUMERIC, freedom NUMERIC,
        generosity NUMERIC, corruption NUMERIC, happiness_score NUMERIC, prediction NUMERIC);"""

    cursor.execute(create_table_query)
    conn.commit()

    cursor.close()
    conn.close()

def process_message(json_msg):
    """Process the message from Kafka producer for prediction and insert into database"""
    df=pd.DataFrame(json_msg, index=[0])

    model = load_model()

    prediction = model.predict(df[["gdp_per_capita", "healthy_life_expectancy", "freedom", "generosity", "corruption" ]])

    json_msg['prediction'] = prediction[0]

    print(json_msg)

    conn = db_connection()
    cur = conn.cursor()
    cur.execute("""INSERT INTO happiness_index (year, country, gdp_per_capita, healthy_life_expectancy, freedom, 
        generosity, corruption, happiness_score, prediction) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", (json_msg['year'], 
        json_msg['country'], json_msg['gdp_per_capita'], json_msg['healthy_life_expectancy'], json_msg['freedom'], json_msg['generosity'], json_msg['corruption'], json_msg['happiness_score'], json_msg['prediction']))
    
    conn.commit()
    cur.close()

def kafka_consumer():
    """Consume the message from Kafka producer"""
    consumer = KafkaConsumer(
        'workshop_3',
        #auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=['localhost:9092']
    )

    try:
        for m in consumer:
            json_msg = m.value
            json_dict = json.loads(json_msg)

            print(json_dict)

            process_message(json_dict)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    create_table()
    kafka_consumer()