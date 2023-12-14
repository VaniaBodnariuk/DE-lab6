import numpy as np
import pandas as pd


def load_csv_to_db(conn):
    table_name = create_table(conn)
    csv_data = read_adjusted_csv_data()

    for index, row in csv_data.iterrows():
        individual_values = tuple(row)
        placeholders = ",".join(["?" for _ in range(len(individual_values))])
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        conn.execute(query, parameters=individual_values)

def create_table(conn):
    conn.execute('''
        CREATE TABLE electric_cars (
            VIN VARCHAR(10),
            County VARCHAR,
            City VARCHAR,
            State VARCHAR,
            Postal_Code INTEGER,
            Model_Year INTEGER,
            Make VARCHAR,
            Model VARCHAR,
            Electric_Vehicle_Type VARCHAR,
            CAFV_Eligibility VARCHAR,
            Electric_Range INTEGER,
            Base_MSRP INTEGER,
            Legislative_District INTEGER,
            DOL_Vehicle_ID INTEGER,
            Vehicle_Location VARCHAR,
            Electric_Utility VARCHAR,
            Census_Tract BIGINT 
        )
    ''')
    return 'electric_cars'

def read_adjusted_csv_data():
    df = pd.read_csv('data/Electric_Vehicle_Population_Data.csv')
    df = df.replace({np.nan: 0})
    return df