from sqlalchemy import create_engine
import sqlite3
import duckdb
import pandas as pd
import time
from config import host, user, password, dbname,command,file
import psycopg2
import sqlite3

post1 = []
post2 = []
post3 = []
post4 = []
lite1 = []
lite2 = []
lite3 = []
lite4 = []
duck1=[]
duck2=[]
duck3=[]
duck4=[]
pan1 = []
pan2 = []
pan3 = []
pan4 = []

def import_post():
    engine=create_engine("postgresql://postgres:21@localhost:5432/postgres")
    tiny= file
    df=pd.read_csv(tiny)
    df.to_sql('trips', engine, if_exists='replace', index=False)

def median(A):
    N = len(A)
    index = N // 2
    if N % 2:
        return sorted(A)[index]
    return sum(sorted(A)[index - 1:index + 1]) / 2

def postgres():
    try:
        conn= psycopg2.connect(
            user=user,
            password=password,
            host=host,
            dbname= dbname,
            port="5432"
        )
        cur = conn.cursor()

        for i in range(10):
            start_time = time.time()
            cur.execute('''SELECT "VendorID", count(*) 
            FROM trips GROUP BY 1;''')
            cur.fetchall()
            end_time = time.time()
            execution_time = end_time - start_time
            post1.append(execution_time)
            start_time = time.time()
            cur.execute('''SELECT passenger_count, avg(total_amount) 
            FROM trips GROUP BY 1;''')
            cur.fetchall()
            end_time = time.time()
            execution_time = end_time - start_time
            post2.append(execution_time)
            start_time = time.time()
            cur.execute('''SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) 
            FROM trips 
            GROUP BY 1, 2;''')
            cur.fetchall()
            end_time = time.time()
            execution_time = end_time - start_time
            post3.append(execution_time)
            start_time = time.time()
            cur.execute( '''SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) 
            FROM trips 
            GROUP BY 1, 2, 3 
            ORDER BY 2, 4 DESC;''')
            cur.fetchall()
            end_time = time.time()
            execution_time = end_time - start_time
            post4.append(execution_time)
        cur.close()
        conn.close()

        print(f"Execution time of query 1 in PostgreSQL: {median(post1)} seconds ")
        print(f"Execution time of query 2 in PostgreSQL: {median(post2)} seconds ")
        print(f"Execution time of query 3 in PostgreSQL: {median(post3)} seconds ")
        print(f"Execution time of query 4 in PostgreSQL: {median(post4)} seconds ")

    except psycopg2.Error as error:
        print("Error: ", error)

def sqlite():
    engine = create_engine("sqlite:///database.db")
    tiny = file
    df = pd.read_csv(tiny)
    df.to_sql('trips', engine, if_exists='replace', index=False)
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    for i in range(10):
        start_time = time.time()
        cur.execute('''SELECT "VendorID", count(*) 
        FROM trips 
        GROUP BY 1;''')
        cur.fetchall()
        end_time = time.time()
        execution_time = end_time - start_time
        lite1.append(execution_time)
        start_time = time.time()
        cur.execute('''SELECT passenger_count, avg(total_amount) 
        FROM trips GROUP BY 1;''')
        cur.fetchall()
        end_time = time.time()
        execution_time = end_time - start_time
        lite2.append(execution_time)
        start_time = time.time()
        cur.execute('''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) 
        FROM trips 
        GROUP BY 1, 2;''')
        cur.fetchall()
        end_time = time.time()
        execution_time = end_time - start_time
        lite3.append(execution_time)
        start_time = time.time()
        cur.execute('''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance), count(*) 
        FROM trips GROUP BY 1, 2, 3 
        ORDER BY 2, 4 DESC;''')
        cur.fetchall()
        end_time = time.time()
        execution_time = end_time - start_time
        lite4.append(execution_time)

    cur.close()
    conn.close()

    print(f"Execution time of query 1 in SQLite: {median(lite1)} seconds ")
    print(f"Execution time  of query  2 in SQLite: {median(lite2)}  seconds ")
    print(f"Execution time  of query  3 in SQLite: {median(lite3)}  seconds ")
    print(f"Execution time  of query  4 in SQLite: {median(lite4)} seconds ")

def duck():
    conn = duckdb.connect(database='trips')
    conn.execute('CREATE TABLE trips (VendorID INTEGER, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count INTEGER, trip_distance FLOAT, RatecodeID INTEGER, store_and_fwd_flag VARCHAR, PULocationID INTEGER, DOLocationID INTEGER, payment_type INTEGER, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT, improvement_surcharge FLOAT, total_amount FLOAT, congestion_surcharge FLOAT, airport_fee FLOAT, Airportfee FLOAT)')
    df = pd.read_csv(file)
    conn.register('trips', df)
    cur = conn.cursor()
    queries = ["SELECT VendorID, count(*) FROM trips GROUP BY 1;","SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;","SELECT passenger_count, SUBSTRING(tpep_pickup_datetime, 1, 4), count(*) FROM trips GROUP BY passenger_count, SUBSTRING(tpep_pickup_datetime, 1, 4);","SELECT passenger_count, SUBSTRING(tpep_pickup_datetime, 1, 4), ROUND(trip_distance), count(*) FROM trips GROUP BY passenger_count, SUBSTRING(tpep_pickup_datetime, 1, 4), ROUND(trip_distance) ORDER BY SUBSTRING(tpep_pickup_datetime, 1, 4), count(*) DESC;"]
    def execution_time(query, conn):
        extimes = []
        for j in range(10):
            start_time = time.time()
            result = conn.execute(query)
            end_time = time.time()
            ex_time=end_time - start_time
            extimes.append(ex_time)
        return median(extimes)
    duckdb_time = []
    i = 1
    for query in queries:
        qtime = execution_time(query, conn)
        duckdb_time.append(qtime)
        print("Execution time for SQL", i,"in DuckDB: ",qtime, " seconds ")
        i += 1
    cur.close()
    conn.close()

def pandas():
    trips = pd.read_csv(file)
    trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'])
    for i in range(10):
        start_time = time.time()
        result1 = trips.groupby("VendorID").size()
        end_time = time.time()
        execution_time = end_time - start_time
        pan1.append(execution_time)
        start_time = time.time()
        result2 = trips.groupby('passenger_count')['total_amount'].mean()
        end_time = time.time()
        execution_time = end_time - start_time
        pan2.append(execution_time)
        start_time = time.time()
        result3 = trips.groupby(['passenger_count', trips['tpep_pickup_datetime'].dt.year]).size()
        end_time = time.time()
        execution_time = end_time - start_time
        pan3.append(execution_time)
        result4 = trips.groupby(['passenger_count', trips['tpep_pickup_datetime'].dt.year, trips['trip_distance'].round()]).size().reset_index(name='count').sort_values(['tpep_pickup_datetime', 'count'], ascending=[True, False])
        end_time = time.time()
        execution_time = end_time - start_time
        pan4.append(execution_time)
        start_time = time.time()
    print(f"Execution time of query 1 in Pandas: {median(pan1)} seconds")
    print(f"Execution time of query 2 in Pandas: {median(pan2)} seconds ")
    print(f"Execution time of query 3 in Pandas: {median(pan3)} seconds ")
    print(f"Execution time of query 4 in Pandas: {median(pan4)} seconds ")

if command==("import_to_postgress"):
    import_post()

if command==("postgres"):
    postgres()

if command==("sqlite"):
    sqlite()

if command==("duckdb"):
    duck()

if command==("pandas"):
    pandas()
