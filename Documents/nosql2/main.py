import psycopg2
from config import config

def connect():
    connection = None
    try: 
        params = config()
        print('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(**params)
        cursor = connection.cursor()
        
        print('Reading and inserting data from TSV file...')
        with open('/home/chokshi/Desktop/data/yago_data.txt', 'r') as file:
            for line in file:
                # Split the line based on the delimiter
                data = line.strip().split(' ')
                # Check if the length of the data is 3
                if len(data) == 3:
                    # Execute the INSERT statement with subject, predicate, and object columns
                    cursor.execute("INSERT INTO triplecollection(subject, predicate, object) VALUES (%s, %s, %s)", data)

        connection.commit()
        cursor.close()
        print('Data insertion completed.')
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None: 
            connection.close()
            print('Database connection terminated.')

connect()