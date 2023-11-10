import pandas as pd
import time

from json import dumps, loads
from kafka import KafkaProducer
from sklearn.model_selection import train_test_split


def csv_transformations_2015():
    """Transform the csv file for 2015"""

    df_2015= pd.read_csv("../data/2015.csv")

    df_2015['year'] = 2015

    deleted_columns = ["Region", "Standard Error", "Family", "Dystopia Residual", "Happiness Rank"]

    df_2015 = df_2015.drop(deleted_columns, axis=1)

    new_order = ['year', 'Country', 'Economy (GDP per Capita)', 'Health (Life Expectancy)', 'Freedom', 'Generosity', 'Trust (Government Corruption)', 'Happiness Score']

    df_2015 = df_2015[new_order]

    df_2015 = df_2015.rename(columns={
    'year': 'year',
    'Country': 'country',
    'Economy (GDP per Capita)': 'gdp_per_capita',
    'Health (Life Expectancy)': 'healthy_life_expectancy',
    'Freedom': 'freedom',
    'Generosity': 'generosity',
    'Trust (Government Corruption)': 'corruption',
    'Happiness Score': 'happiness_score'
    })

    df_2015.head()

    return df_2015


def csv_transformations_2016():
    """Transform the csv file for 2016"""

    df_2016= pd.read_csv("../data/2016.csv")

    df_2016['year'] = 2016

    deleted_columns = ["Region", "Lower Confidence Interval", "Upper Confidence Interval", "Family", "Dystopia Residual", "Happiness Rank"]

    df_2016 = df_2016.drop(deleted_columns, axis=1)

    new_order = ['year', 'Country', 'Economy (GDP per Capita)', 'Health (Life Expectancy)', 'Freedom', 'Generosity', 'Trust (Government Corruption)', 'Happiness Score']

    df_2016 = df_2016[new_order]

    df_2016 = df_2016.rename(columns={
    'year': 'year',
    'Country': 'country',
    'Economy (GDP per Capita)': 'gdp_per_capita',
    'Health (Life Expectancy)': 'healthy_life_expectancy',
    'Freedom': 'freedom',
    'Generosity': 'generosity',
    'Trust (Government Corruption)': 'corruption',
    'Happiness Score': 'happiness_score'
    })

    df_2016.head()

    return df_2016


def csv_transformations_2017():
    """Transform the csv file for 2017"""

    df_2017= pd.read_csv("../data/2017.csv")

    df_2017['year'] = 2017

    deleted_columns = ["Whisker.high", "Whisker.low", "Family", "Dystopia.Residual", "Happiness.Rank"]

    df_2017 = df_2017.drop(deleted_columns, axis=1)

    new_order = ['year', 'Country', 'Economy..GDP.per.Capita.', 'Health..Life.Expectancy.', 'Freedom', 'Generosity', 'Trust..Government.Corruption.', 'Happiness.Score']

    df_2017 = df_2017[new_order]

    df_2017 = df_2017.rename(columns={
    'year': 'year',
    'Country': 'country',
    'Economy..GDP.per.Capita.': 'gdp_per_capita',
    'Health..Life.Expectancy.': 'healthy_life_expectancy',
    'Freedom': 'freedom',
    'Generosity': 'generosity',
    'Trust..Government.Corruption.': 'corruption',
    'Happiness.Score': 'happiness_score'
    })

    df_2017.head()

    return df_2017  


def csv_transformations_2018():
    """Transform the csv file for 2018"""

    df_2018= pd.read_csv("../data/2018.csv")

    df_2018['year'] = 2018

    null_value_row = df_2018[df_2018['Perceptions of corruption'].isnull()]

    df_2018.loc[null_value_row.index, 'Perceptions of corruption'] = 0.182

    deleted_columns = ["Overall rank", "Social support"]

    df_2018 = df_2018.drop(deleted_columns, axis=1)

    new_order = ['year', 'Country or region', 'GDP per capita', 'Healthy life expectancy', 'Freedom to make life choices', 'Generosity', 'Perceptions of corruption', 'Score']

    df_2018 = df_2018[new_order]

    df_2018 = df_2018.rename(columns={
    'year': 'year',
    'Country or region': 'country',
    'GDP per capita': 'gdp_per_capita',
    'Healthy life expectancy': 'healthy_life_expectancy',
    'Freedom to make life choices': 'freedom',
    'Generosity': 'generosity',
    'Perceptions of corruption': 'corruption',
    'Score': 'happiness_score'
    })

    df_2018.head()

    return df_2018



def csv_transformations_2019():
    """Transform the csv file for 2019"""

    df_2019= pd.read_csv("../data/2019.csv")

    df_2019['year'] = 2019

    deleted_columns = ["Overall rank", "Social support"]

    df_2019 = df_2019.drop(deleted_columns, axis=1)

    new_order = ['year', 'Country or region', 'GDP per capita', 'Healthy life expectancy', 'Freedom to make life choices', 'Generosity', 'Perceptions of corruption', 'Score']

    df_2019 = df_2019[new_order]

    df_2019 = df_2019.rename(columns={
    'year': 'year',
    'Country or region': 'country',
    'GDP per capita': 'gdp_per_capita',
    'Healthy life expectancy': 'healthy_life_expectancy',
    'Freedom to make life choices': 'freedom',
    'Generosity': 'generosity',
    'Perceptions of corruption': 'corruption',
    'Score': 'happiness_score'
    })

    df_2019.head()

    return df_2019

def concatenate_all_info(df1, df2, df3, df4, df5):
    """Concatenate all the dataframes"""

    df_2015= df1
    df_2016= df2
    df_2017= df3
    df_2018= df4
    df_2019= df5

    df_all = pd.concat([df_2015, df_2016, df_2017, df_2018, df_2019], ignore_index=True)

    df_all.head()

    return df_all


def kafka_producer(df):
    """Send the message to Kafka topic"""
    producer = KafkaProducer(
        value_serializer = lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers = ['localhost:9092'],
    )


    features = ["gdp_per_capita", "healthy_life_expectancy", "freedom", "generosity", "corruption"]
    target = "happiness_score"
    X = df[features]
    y = df[target]


    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=13)

    df=df.loc[X_test.index]

    print(df.shape)


    for i in range (len(df)):

        row_json = df.iloc[i].to_json()

        producer.send("workshop_3", value=row_json)
        print(row_json)
        #time.sleep(1)


if __name__ == "__main__":

    df_2015 = csv_transformations_2015()
    df_2016 = csv_transformations_2016()
    df_2017 = csv_transformations_2017()
    df_2018 = csv_transformations_2018()
    df_2019 = csv_transformations_2019()

    df_all = concatenate_all_info(df_2015, df_2016, df_2017, df_2018, df_2019)

    df_all.head()

    kafka_producer(df_all)




    
