import socket
import json
import pprint

class Client:
    def __init__(self, client_id):
        self.client_id = client_id
        self.connected_server_id = None
        self.client_socket = None
        self.ID_to_PORT_dict = {
            1: 3000,  #Mongo Server
            2: 30001, #hive server
            3:5433 #postgre
            #Add entries for other servers if needed
        }

    def connect_to_server(self, server_id):
        print(f'Establishing connection to server: {server_id}')
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect(('localhost', self.ID_to_PORT_dict[server_id]))
        print(f"Connected to server on localhost:{self.ID_to_PORT_dict[server_id]}")
        self.connected_server_id = server_id

    def query(self, subject):
        query_data = {
            'request_type': 'query',
            'client_id': self.client_id,
            'subject': subject,
        }
        json_string = json.dumps(query_data)
        self.client_socket.sendall(json_string.encode())
        print("Query sent to server.")
        # Receive response from server
        response_json = self.client_socket.recv(1024)
        # Parse json response
        response_dict = json.loads(response_json)
        print("Response from server:")
        pprint.pprint(response_dict)

    def update(self, subject,predicate,object):
        query_data = {
            'request_type': 'update',
            'client_id': self.client_id,
            'subject': subject,
            'predicate': predicate,
            'object':object

        }
        json_string = json.dumps(query_data)
        self.client_socket.sendall(json_string.encode())
        print("Update Query sent to server.")
        # Receive response from server
        response_json = self.client_socket.recv(1024)
        # Parse json response
        response_dict = json.loads(response_json)
        print("Response from server:")
        if(response_dict['ack']):
            print("update success")

    def merge(self,server_id):
        query_data = {
            'request_type': 'merge',
            'server_id': server_id 
        }
        json_string = json.dumps(query_data)
        self.client_socket.sendall(json_string.encode())
        print("Sending merge requests to other servers\n")
        json_data = self.client_socket.recv(1024)
        try:
            json_obj = json.loads(json_data)
            if json_obj['ack']:
                print("Update done\n")
                return
        except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                return

    def close_connection(self):
        if self.client_socket:
            end_req = {
                'request_type': 'end',
                'client_id': self.client_id,
            }
            json_string = json.dumps(end_req)
            self.client_socket.sendall(json_string.encode())
            self.client_socket.close()
            print("Connection closed.")

    def run(self):
        print('Running client')
        print('='*20)
        while True:
            print('Enter 0 -> Exit')
            print('Server ID 1 -> Mongo Server')
            print('Server ID 2 -> Hive Server')
            print('Server ID 3-> SQL Server')
            server_id = int(input('Enter Server ID: '))
            if server_id == 0:
                print('Exiting')
                #self.close_connection()
                return
            if server_id in self.ID_to_PORT_dict.keys():
                self.connect_to_server(server_id)
            else:
                print(f'Server ID: {server_id} is INVALID, try again or exit')
                break


            print('='*20)
            while True:
                print('Enter 0 -> Exit')
                print('Enter 1 -> Query')
                print('Enter 2 -> Update')
                print('Enter 3 -> Merge')
                ip = int(input('Enter input: '))
                if ip == 0:
                    self.close_connection()
                    break
                    
                elif ip == 1:
                    subject = str(input('Enter subject: '))
                    self.query(subject)
                elif ip==2:
                    sub=str(input('Enter subject:'))
                    pred=str(input('Enter predicate'))
                    obj=str(input('Enter object:'))
                    self.update(sub,pred,obj)
                elif ip == 3:
                    sid=int(input('Enter the server id u want to merge with:'))
                    self.merge(sid)


if __name__ == '__main__':
    client = Client(1)
    client.run()