import json
import pandas as pd
import psycopg2 as psy

from sklearn.metrics import mean_squared_error, r2_score


def db_connection():
    """Connect to the database"""

    with open('./secrets/config_db.json') as config_json:
        config = json.load(config_json)
    conn = psy.connect(**config) 
    return conn


def find_metrics():
    """Find the metrics of the model"""
    conn = db_connection()

    df = pd.read_sql_query("SELECT * FROM happiness_index", conn)

    conn.close()

    y_test = df['happiness_score']
    y_pred = df['prediction']
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print("MSE:", mse)
    print("r2:", r2)


if __name__ == "__main__":
    find_metrics()