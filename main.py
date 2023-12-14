import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from db_loader import load_csv_to_db

def main():
    conn = duckdb.connect(database=':memory:', read_only=False)
    load_csv_to_db(conn)
    count_cars_by_city(conn)
    top_3_models(conn)
    top_model_by_postal_code(conn)

    result = get_count_cars_by_year(conn)
    write_files_partitioned_by_year(conn, result)
    conn.close()

def count_cars_by_city(conn):
    result = conn.execute('SELECT Model_Year, City, COUNT(*) AS Car_Count FROM electric_cars GROUP BY Model_Year, City')
    print("1")
    write_result_to_parquet(result, 'count_cars_by_city')

def top_3_models(conn):
    result = conn.execute('SELECT Model, COUNT(*) AS Car_Count FROM electric_cars GROUP BY Model ORDER BY Car_Count DESC LIMIT 3')
    write_result_to_parquet(result, 'top_3_models')

def top_model_by_postal_code(conn):
    result = conn.execute('''
        SELECT Postal_Code, Model, MAX(Car_Count) AS Max_Car_Count
        FROM (
            SELECT Postal_Code, Model, COUNT(*) AS Car_Count
            FROM electric_cars
            GROUP BY Postal_Code, Model
        ) t
        GROUP BY Postal_Code, Model
    ''')
    write_result_to_parquet(result,  'top_model_by_postal_code')

def get_count_cars_by_year(conn):
    return conn.execute('SELECT Model_Year, COUNT(*) AS Car_Count FROM electric_cars GROUP BY Model_Year')

def write_files_partitioned_by_year(conn, result):
    for Model_Year, Car_Count in result.items():
        result = conn.execute(f'SELECT * FROM electric_cars WHERE Model_Year = {Model_Year}')
        write_result_to_parquet(result, f'count_cars_by_year/{Car_Count}_electric_cars_in_{Model_Year}')

def write_result_to_parquet(result, filename):
    df = result.fetch_df()
    pq.write_table(pa.table(df), f'results/{filename}.parquet')

if __name__ == "__main__":
    main()
