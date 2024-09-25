import pymongo
import socket
import json
import psycopg2
from datetime import datetime, timezone
from pyhive import hive
import time

class MongoServer:
    def __init__(self, db_name, collection_name):
        print('initializing server')
        self.client = pymongo.MongoClient("localhost", 27017)  
        print('connected to mongoDB server on localhost:27017')
        self.db = self.client[db_name]
        self.collection_name = collection_name
        print(f'accessing DB: {db_name}')
        self.tripleStoreSchema = {
            'subject' : str,
            'predicate' : str,
            'object' : str
        }
        self.PORT = 3000
        self.ID_to_PORT_dict = {
            1 : 3000,
            2 : 30000,
        }
        self.client_id = 0
        
    
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



    def update(self, subject, predicate, obj,timestamp):
        collection = self.db[self.collection_name]

        # Check if an entry with the same subject and predicate exists
        delete_result = collection.delete_many({'subject': subject, 'predicate': predicate})
        print(f'{delete_result.deleted_count} entries deleted')

# Insert a new entry
        insert_result = collection.insert_one({
        'subject': subject,
        'predicate': predicate,
        'obj': obj
        })
        if insert_result.acknowledged:
            print('New entry added')

       # timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')  # Get current UTC time
        update_collection = self.db['updates']
        update_collection.delete_many({'subject': subject, 'predicate': predicate})
        insert_result = update_collection.insert_one({
                'timestp': timestamp,
                'subject': subject,
                'predicate': predicate,
                'object': obj
            })
        if insert_result.acknowledged:
            print('New entry added into the update log')
        else:
            print('Insert failed')
        return 0

    def query(self, subject):
        print('querying monodb server')
        collection = self.db[self.collection_name]
        results_list = []
        query_condition = {'subject': subject}
        for entry in collection.find(query_condition):
            entry['_id'] = str(entry['_id'])
            results_list.append(entry)

        return results_list
    
    
    def merge(self, server_id):
        if server_id == 2:
            self.hive_conn = hive.Connection(host='localhost', port=10000, database='yago_db')
            hive_cursor = self.hive_conn.cursor()
            hive_cursor.execute(f"SELECT * FROM updates_table ORDER BY timestp DESC")
            hive_updates = hive_cursor.fetchall()

            for row in hive_updates:
                timestp,subject, predicate, object = row
                mongo_row = self.db['updates'].find_one({"subject": subject, "predicate": predicate})
                if mongo_row and timestp > mongo_row.get("timestp", "0000-00-00-00-00-00"):
                    self.update(subject,predicate,object,timestp)
                elif not mongo_row:
                    self.update(subject,predicate,object,timestp)

            hive_cursor.close()
        elif server_id==3:
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
            
            for row in postgres_data:
                timestp, subject, predicate, obj = row
                mongo_row = self.db['updates'].find_one({"subject": subject, "predicate": predicate})
                if mongo_row and timestp > mongo_row.get("timestp", "0000-00-00-00-00-00"):
                    self.update(subject,predicate,obj,timestp)
                elif not mongo_row:
                    self.update(subject,predicate,obj,timestp)



    def run(self):
        print('running server')
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('localhost', self.PORT))
        server_socket.listen(5)  
        while True:
            print(f"Server is listening on port {self.PORT}")
            client_socket, client_address = server_socket.accept()  
            while True:
                print(f"Connection from {client_address} has been established!")
                print('Listening for client requests')
                # Recieve JSON object from client
                json_data = client_socket.recv(1024).decode()
                # print("Received data from client:", json_data)  
                # parsing json request
                try:
                    json_obj = json.loads(json_data)
                    if json_obj['request_type'] == 'query':
                        subject = json_obj['subject']
                        client_id = json_obj['client_id']
                        print(f'Query request from client: {client_id}, subject: {subject}')
                        query_result = self.query(subject)
                        print(f'query complete, {len(query_result)} objects retrieved')
                        response = json.dumps(query_result)
                        client_socket.sendall(response.encode())
                        print('results sent to client')
                        continue
                    if json_obj['request_type'] == 'end':
                        print(f'closing connection with {client_address}')
                        break
                    if json_obj['request_type'] == 'update':
                        subject = json_obj['subject']
                        predicate = json_obj['predicate']
                        obj = json_obj['object']
                        #server_id = json_obj['server_id']
                        print(f'Update request from client: subject: {subject}, predicate: {predicate}, object: {obj}')
                        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S') 
                        self.update(subject, predicate, obj,timestamp)
                        response = json.dumps({
                            'ack': True
                        })
                        client_socket.sendall(response.encode())
                        print('Done')
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

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    break


if __name__ == '__main__':
    server = MongoServer('sample_db', 'tripleCollection')
    server.run()
    # print(server.query('<Barney_Stinson>'))
