from datetime import datetime
import pymongo
import psycopg2
from pyhive import hive
import socket
import json

class HiveServer:
    def __init__(self, db_name, table_name):
        print('Initializing server')
        self.conn = hive.connect(host='localhost', port=10000, database=db_name)
        self.cursor = self.conn.cursor()
        print(f'Connected to Hive server on localhost:10000')
        self.db_name = db_name
        self.table_name = table_name
        self.PORT = 30001

    def query(self, subject):
        print('Querying Hive server')
        query = f"SELECT * FROM {self.table_name} WHERE subject = '{subject}'"
        self.cursor.execute(query)
        results_list = [dict(zip([column[0] for column in self.cursor.description], row)) for row in self.cursor.fetchall()]
        return results_list

    def update(self, subject, predicate, obj,timestamp):
        print('Updating Hive server')
        #update_query =f"INSERT OVERWRITE TABLE {self.table_name} SELECT subject, predicate, CASE WHEN subject = '{subject}' AND predicate = '{predicate}' THEN '{obj}' ELSE object END FROM {self.table_name}"
        delete_query = f"""
            INSERT OVERWRITE TABLE {self.table_name}
            SELECT * FROM {self.table_name}
            WHERE subject != '{subject}' OR predicate != '{predicate}'
            """
        self.cursor.execute(delete_query)
        update_query=f"INSERT INTO {self.table_name} (subject, predicate, object) VALUES ('{subject}', '{predicate}', '{obj}')"
        self.cursor.execute(update_query)
        self.conn.commit()
        print('Entry updated')
        self.cursor.execute(f"SELECT * FROM updates_table WHERE subject='{subject}' AND predicate='{predicate}'")
        existing_row = self.cursor.fetchone()

        if existing_row:
            # Update existing row using INSERT OVERWRITE
            delete_query = f"""
            INSERT OVERWRITE TABLE updates_table
            SELECT * FROM updates_table
            WHERE subject != '{subject}' OR predicate != '{predicate}'
            """
            self.cursor.execute(delete_query)
    
            # Insert new row with current timestamp
        #timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        insert_query = f"INSERT INTO updates_table (timestp, subject, predicate, object) VALUES ('{timestamp}', '{subject}', '{predicate}', '{obj}')"
        self.cursor.execute(insert_query)

        self.conn.commit()
        print('Entry updated or inserted')

    def merge(self, server_id):
        if server_id == 1:
            mongo_client = pymongo.MongoClient('localhost', 27017)
            mongo_db = mongo_client['sample_db']
            mongo_collection = mongo_db['updates']
            mongo_data = mongo_collection.find().sort('timestp', pymongo.DESCENDING)
            hive_conn = hive.Connection(host='localhost', port=10000, database='yago_db')
            hive_cursor = hive_conn.cursor()

            for doc in mongo_data:
                subject = doc['subject']
                predicate = doc['predicate']
                obj = doc['object']
                timestp = doc['timestp']
                hive_cursor.execute(f"SELECT * FROM updates_table WHERE subject='{subject}' AND predicate='{predicate}'")
                hive_row = hive_cursor.fetchone()

                if (hive_row and timestp > hive_row[0]):
                    self.update(subject,predicate,obj,timestp)
                elif hive_row is None:
                    self.update(subject,predicate,obj,timestp)

            hive_cursor.close()

            mongo_client.close()
        
        elif server_id==3:
    # Connect to PostgreSQL
            conn = psycopg2.connect(
            dbname='yago',
            user='postgres',
            password=12345,
            host='localhost'
        )
            cursor = conn.cursor()

    # Retrieve data from PostgreSQL
            cursor.execute("SELECT * FROM updates ORDER BY timestp DESC")
            postgres_data = cursor.fetchall()

    # Connect to Hive
            hive_conn = hive.Connection(host='localhost', port=10000, database='yago_db')
            hive_cursor = hive_conn.cursor()

            for row in postgres_data:
                timestp, subject, predicate, obj = row

        # Check if the row already exists in Hive
                hive_cursor.execute(f"SELECT * FROM updates_table WHERE subject='{subject}' AND predicate='{predicate}'")
                hive_row = hive_cursor.fetchone()

                if (hive_row and timestp > hive_row[0]):
                    self.update(subject,predicate,obj,timestp)
                elif hive_row is None:
                    self.update(subject,predicate,obj,timestp)
            # Update row in

            conn.close()
            hive_cursor.close()
            




    def run(self):
        print('Running server')
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('localhost', self.PORT))
        server_socket.listen(5)

        while True:
            print(f"Server is listening on port 30000")
            client_socket, client_address = server_socket.accept()

            while True:
                print(f"Connection from {client_address} has been established!")
                print('Listening for client requests')
                
                json_data = client_socket.recv(1024).decode()
                try:
                    json_obj = json.loads(json_data)
                    if json_obj['request_type'] == 'query':
                        subject = json_obj['subject']
                        client_id = json_obj['client_id']
                        print(f'Query request from client: {client_id}, subject: {subject}')
                        query_result = self.query(subject)
                        print(f'Query complete, {len(query_result)} objects retrieved')
                        response = json.dumps(query_result)
                        client_socket.sendall(response.encode())
                        print('Results sent to client')
                        continue
                    if json_obj['request_type'] == 'update':
                        subject = json_obj['subject']
                        predicate = json_obj['predicate']
                        obj = json_obj['object']
                        client_id = json_obj['client_id']
                        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
                        print(f'Update request from client: {client_id}, subject: {subject}, predicate: {predicate}, object: {obj}')
                        self.update(subject, predicate, obj,timestamp)
                        response = json.dumps({
                            'ack': True
                        })
                        client_socket.sendall(response.encode())
                        print('Update successful')
                        continue
                    if json_obj['request_type'] == 'merge':
                        server_id = json_obj['server_id']
                        print(f'Merge query from client, server_id: {server_id}')
                        self.merge(server_id)
                        response = json.dumps({
                            'ack': True
                        })
                        client_socket.sendall(response.encode())
                        continue
                    if json_obj['request_type'] == 'end':
                        print(f'Closing connection with {client_address}')
                        break
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    continue

if __name__ == '__main__':
    print('hello')
    server = HiveServer(db_name='yago_db',table_name='yago_dataset')
    server.run()