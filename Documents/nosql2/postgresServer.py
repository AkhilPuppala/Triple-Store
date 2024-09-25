import psycopg2
import pymongo
from pyhive import hive
from config import config
import socket
import json
from datetime import datetime, timezone

class Server:
    def __init__(self, db_name):
        print('initializing server')
        try:
            params = config()
            print('Connecting to the PostgreSQL database...')
            self.connection = psycopg2.connect(**params)
            self.cursor = self.connection.cursor()
            print("connected")
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        self.db_name = db_name
        print(f'accessing DB: {db_name}')
        self.tripleStoreSchema = {
            'subject' : str,
            'predicate' : str,
            'object' : str
        }
        self.PORT = 5433
        self.ID_to_PORT_dict = {
            1 : 3000,
            2 : 30001,
            3 : 5433,
        }
        self.client_id = 0


    def query(self, subject):
        try:
            if self.connection is None:
                self.connect()
            self.cursor.execute("SELECT * FROM triplecollection WHERE subject = %s", (subject,))
            rows = self.cursor.fetchall()
            return rows
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)


    def connectToServer(self,server_id):
        print(f'Establishing connection to server: {server_id}')
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect(('localhost', self.ID_to_PORT_dict[server_id]))
        print(f"Connected to server on localhost:{self.ID_to_PORT_dict[server_id]}")
        self.connected_server_id = server_id

    def closeConnection(self):
        if self.client_socket:
            end_req = {
                'request_type': 'end',
                'client_id': self.client_id,
            }
            json_string = json.dumps(end_req)
            self.client_socket.sendall(json_string.encode())          
            self.client_socket.close()
            print("Connection closed.")


    def update(self, subject, predicate, new_object,timestamp):
        try:
            # Delete existing entry with same subject and predicate
            self.cursor.execute(("DELETE FROM triplecollection WHERE subject = %s AND predicate = %s"), (subject, predicate))
            deleted_count = self.cursor.rowcount
            print(f'{deleted_count} entries deleted')

            # Insert new entry
            self.cursor.execute(("INSERT INTO triplecollection (subject, predicate, object) VALUES (%s, %s, %s)"), (subject, predicate, new_object))
            self.connection.commit()
            print('New entry added')

            # Insert into update log
            
            self.cursor.execute(("DELETE FROM updates WHERE subject = %s AND predicate = %s"), (subject, predicate))
            self.cursor.execute(("INSERT INTO updates (timestp, subject, predicate, object) VALUES (%s, %s, %s, %s)"), (timestamp, subject, predicate, new_object))
            self.connection.commit()
            print('New entry added into the update log')
        except Exception as e:
            print(f'Error: {e}')


    def merge(self, server_id):
        if server_id == 1:
    # Connect to MongoDB
            mongo_client = pymongo.MongoClient('localhost', 27017)
            mongo_db = mongo_client['sample_db']
            mongo_collection = mongo_db['updates']

    # Retrieve data from MongoDB
            mongo_data = mongo_collection.find().sort('timestp', pymongo.DESCENDING)

    # Connect to PostgreSQL
            conn = psycopg2.connect(
            dbname='yago',
            user='postgres',
            password=12345,
            host='localhost'
            )
            cursor = conn.cursor()

            for doc in mongo_data:
                timestp = doc['timestp']
                subject = doc['subject']
                predicate = doc['predicate']
                obj = doc['object']

        # Check if the row already exists in PostgreSQL
                cursor.execute(f"SELECT * FROM updates WHERE subject='{subject}' AND predicate='{predicate}'")
                postgres_row = cursor.fetchone()

                if postgres_row and timestp > postgres_row[0]:
            # Update row in PostgreSQL
                    self.update(subject,predicate,obj,timestp)
                elif not postgres_row:
                    self.update(subject,predicate,obj,timestp)
            # Insert new row into PostgreSQL
                

            conn.commit()
            conn.close()
            mongo_client.close()

        elif server_id==2:
    # Connect to Hive
            hive_conn = hive.Connection(host='localhost', port=10000, database='yago_db')
            hive_cursor = hive_conn.cursor()

    # Retrieve data from Hive
            hive_cursor.execute("SELECT * FROM updates_table ORDER BY timestp DESC")
            hive_data = hive_cursor.fetchall()

    # Connect to PostgreSQL
            conn = psycopg2.connect(
                dbname='yago',
                user='postgres',
                password=12345,
                host='localhost'
                )
            cursor = conn.cursor()

            for row in hive_data:
                timestp, subject, predicate, obj = row

        # Check if the row already exists in PostgreSQL
                cursor.execute(f"SELECT * FROM updates WHERE subject='{subject}' AND predicate='{predicate}'")
                postgres_row = cursor.fetchone()

                if (postgres_row and timestp > postgres_row[0]):
            # Update row in PostgreSQL
                    self.update(subject,predicate,obj,timestp)
                elif not postgres_row:
                    self.update(subject,predicate,obj,timestp)

            conn.commit()
            conn.close()
            hive_cursor.close()

    def run(self):
        print('Running Server')
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('localhost', self.PORT))
        server_socket.listen(5)
        while True:
            print(f"Server is listening on port {self.PORT}")
            client_socket, client_address = server_socket.accept()  
            while True:
                print(f"Connection from {client_address} has been established!")
                print('Listening for client requests')
                json_data = client_socket.recv(2048).decode()
                print(json_data)
                try:
                    json_obj = json.loads(json_data)

                    if json_obj['request_type'] == 'query':
                        subject = json_obj['subject']
                        client_id = json_obj['client_id']
                        print(f'Query request from client: {client_id}, subject: {subject}')
                        query_result = self.query(subject)
                        # print(f'query complete, {len(query_result)} objects retrieved')
                        response = json.dumps(query_result)
                        client_socket.sendall(response.encode())
                        print('results sent to client')
                        continue

                    elif json_obj['request_type'] == 'update':
                        subject = json_obj['subject']
                        client_id = json_obj['client_id']
                        predicate = json_obj['predicate']
                        obj = json_obj['object']
                        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
                        print(f'Update request from client: {client_id}, subject: {subject}, predicate: {predicate}, new Object: {obj}')
                        self.update(subject, predicate, obj,timestamp)
                        print('Update completed')
                        response = json.dumps({
                            'ack': True
                        })
                        client_socket.sendall(response.encode())
                        continue

                    elif json_obj['request_type'] == 'end':
                        print(f'closing connection with {client_address}')
                        #self.connection.close()
                        print('Database connection terminated.')
                            # Reset connection and cursor variables
                        #self.connection = None
                        #self.cursor = None
                        break
                    elif json_obj['request_type'] == 'merge':
                        server_id = json_obj['server_id']
                        print(f'Merge query from client, server_id: {server_id}')
                        self.merge(server_id)
                        response = json.dumps({
                            'ack': True
                        })
                        client_socket.sendall(response.encode())
                        continue                       

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    break




# Example usage:
if __name__ == '__main__':
    server = Server('Nosql_project')
    server.run()
