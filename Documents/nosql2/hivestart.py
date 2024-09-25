from pyhive import hive

class HiveTripleStore:
    def __init__(self, host, port, database):
        self.host = host
        self.port = port
        self.database = database
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = hive.connect(host=self.host, port=self.port, database=self.database)
        self.cursor = self.conn.cursor()

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def create_database(self):
        query = f"CREATE DATABASE IF NOT EXISTS {self.database}"
        self.cursor.execute(query)

    def create_table(self, table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
        subject STRING,
        predicate STRING,
        object STRING)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ' '
        """
        self.cursor.execute(query)
        self.cursor.execute("SHOW TABLES")
        tables = [table[0] for table in self.cursor.fetchall()]
        return tables

    def create_updates_table(self, table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
        timestp STRING,
        subject STRING,
        predicate STRING,
        object STRING)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ' '
        """
        self.cursor.execute(query)

    def load_data(self, table_name, file_path):
        query = f"LOAD DATA LOCAL INPATH '{file_path}' INTO TABLE {table_name}"
        self.cursor.execute(query)

    def query(self, table_name, subject):
        query = f"SELECT * FROM {table_name} where subject = '{subject}'"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def update(self, table_name, subject, predicate, obj):
        update_query =f"INSERT OVERWRITE TABLE {table_name} SELECT subject, predicate, CASE WHEN subject = '{subject}' AND predicate = '{predicate}' THEN '{obj}' ELSE object END FROM {table_name}"
        self.cursor.execute(update_query)
        self.conn.commit()
        print('Entry updated')

    def merge(self, server_id):
        # Implement merge functionality if needed
        pass

# Example usage
if __name__ == "__main__":
    hive_host = 'localhost'    
    hive_port = 10000
    database_name = 'yago_db'  # Change this to the desired database name

# Connect to default database (usually 'default')
    hive_conn = hive.connect(host=hive_host, port=hive_port)

# Create a cursor to execute commands
    cursor = hive_conn.cursor()

# Check if the database already exists
    cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
    exists = cursor.fetchone()

    if not exists:
    # Create the database if it doesn't exist
        cursor.execute(f"CREATE DATABASE {database_name}")

# Close the cursor and connection
   # cursor.close()
    hive_conn.close()
    hive_store = HiveTripleStore(host='localhost', port=10000, database='yago_db')
    hive_store.connect()

    # Create database if not exists

    # Create table if not exists
    table_name = 'yago_dataset'
    table_name2='updates_table'
    hive_store.create_updates_table(table_name2)
    tables=hive_store.create_table(table_name)
    #
    hive_store.cursor.execute(f"SELECT * FROM updates_table")
    x=hive_store.cursor.fetchall()
    print(x)
    print(tables)

    # Load data into the table
    hive_store.load_data(table_name, '/home/chokshi/Desktop/data/yago_data.txt')

    # Example query
    # results = hive_store.query(table_name, '<Allen_Tough>')
    # print("Query Results:")
    # for row in results:
    #     print(row)

    # Example update
    #hive_store.update(table_name, '<Allen_Tough>', '<hasGivenName>', '<charan>')

    hive_store.disconnect()
