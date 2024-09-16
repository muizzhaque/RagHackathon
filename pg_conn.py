import psycopg2
from config import database_names
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

class PG:
    """
    A class representing a PostgreSQL connection and operations.

    Attributes:
        conn: The PostgreSQL connection object.
        cursor: The cursor object for executing SQL queries.
        conn2: The connection object for the `to_sql` function.
    """

    def __init__(self,name_of_db):
        self.pg_connect(name_of_db)

    def pg_connect(self,name_of_db):
        """
        Establishes a connection to the PostgreSQL database.

        Args:
            database: The name of the database to connect to.
        """
        conn_string = "host={0} port=5432 user={1} dbname={2} password={3}".format(database_names[name_of_db]['host'],database_names[name_of_db]['user'],database_names[name_of_db]['dbname'],database_names[name_of_db]['password'])
        self.conn = psycopg2.connect(conn_string)
        print("Connection established to database: {}".format(name_of_db))
        self.cursor = self.conn.cursor()

        # establish connections for to_sql function
        conn_string2 = 'postgresql://{1}:{3}@{0}/{2}'.format(database_names[name_of_db]['host'],database_names[name_of_db]['user'],database_names[name_of_db]['dbname'],quote_plus(database_names[name_of_db]['password']))
        db = create_engine(conn_string2)
        self.conn2 = db.connect()

    # Function to check if a table exists
    def table_exists(self, table_name):
        self.cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = %s)", (table_name,))
        return self.cursor.fetchone()[0]

    def select_sql(self, sql_string):
        """
        Executes a SELECT SQL query and returns the result as a DataFrame.

        Args:
            sql_string: The SELECT SQL query to execute.

        Returns:
            A DataFrame containing the result of the query.
        """
        self.cursor.execute(sql_string)
        rows = self.cursor.fetchall()
        rows =  pd.DataFrame(rows)
        columns = [desc[0] for desc in self.cursor.description]
        if len(rows)>0:
            rows.columns = columns
        return rows
        
    def select_sql_for_large_tables(self, sql_string):
        self.cursor.execute(sql_string)
        columns = [desc[0] for desc in self.cursor.description]
        table_chunks = []
        while True:
            # Check available memory and determine chunk size dynamically
            # mem = psutil.virtual_memory()
            # chunk_size = int(mem.available / (200))
            chunk_size  = 1000000
            rows = self.cursor.fetchmany(chunk_size)
            if not rows:
                break
            df = pd.DataFrame(rows)
            if len(df)>0:
                df.columns = columns
            table_chunks.append(df)
        # final_df = pd.concat(table_chunks, ignore_index=True)
        # if len(final_df)>0:
        #     final_df.columns = columns
        #     print(rows)
        return table_chunks

    def execute_sql_value(self, sql_string, value):
        """
        Executes a SQL query with values provided.

        Args:
            sql_string: The SQL query to execute.
        """
        self.cursor.execute(sql_string, value)

    def execute_sql(self, sql_string):
        """
        Executes a SQL query.

        Args:
            sql_string: The SQL query to execute.
        """
        self.cursor.execute(sql_string)

    def get_table_columns(self, table_name):
        self.cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
            """)
        columns = [row[0] for row in self.cursor.fetchall()]
        return set(columns)

    def to_psql(self, df, table):
        """
        Inserts a DataFrame into a PostgreSQL table.

        Args:
            df: The DataFrame to insert.
            table: The name of the table to insert into.
        """
        print(df.to_sql(table, self.conn2, if_exists= 'append', index=False))

    def to_psql_in_chunks(self, df, table, chunksize=100):
        """
        Inserts a DataFrame into a PostgreSQL table in chunks.

        Args:
            df: The DataFrame to insert.
            table: The name of the table to insert into.
            chunksize: The number of rows to insert at a time.
        """
        df.to_sql(table, self.conn2, if_exists='append', chunksize=chunksize, index=False)

    def update_to_psql(self, df, table):
        print(df.to_sql(table, self.conn2, if_exists= 'replace'))

    def update_to_psql_in_chunks(self, df, table, chunksize=100):
        """
        Updates a PostgreSQL table with data from a DataFrame in chunks.

        Args:
            df: The DataFrame containing the updated data.
            table: The name of the table to update.
            chunksize: The number of rows to update at a time.
        """
        df.to_sql(table, self.conn2, if_exists='replace', chunksize=chunksize, index=False)

    def insert_row(self, cursor, table_name, row):
        """
        Inserts a row into a PostgreSQL table.

        Args:
            cursor: The cursor object for executing SQL queries.
            table_name: The name of the table to insert into.
            row: The row to insert.
        """
        values_placeholder = ", ".join(["%s"] * len(row.values))
        insert_query = f"INSERT INTO {table_name} VALUES ({values_placeholder})"
        cursor.execute(insert_query, tuple(row.values))

    def republish_dataframe_to_postgres(self, dataframe, table_name):
        """
        Republishes a DataFrame to a PostgreSQL table.

        Args:
            dataframe: The DataFrame to republish.
            table_name: The name of the table to republish to.
        """
        self.cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
        table_exists = self.cursor.fetchone()[0]
        
        if table_exists:
            self.cursor.execute(f"DROP TABLE {table_name}")
        
        columns = ", ".join([f"{column} {dataframe[column].dtype}" for column in dataframe.columns])
        create_table_query = f"CREATE TABLE {table_name} ({columns})"
        create_table_query = create_table_query.replace("int64", "bigint")
        create_table_query = create_table_query.replace("float64", "double precision")
        create_table_query = create_table_query.replace("object", "text")
        
        self.cursor.execute(create_table_query)
        self.conn.commit()
        self.to_psql_in_chunks(dataframe, table_name, 20)

    # Clean up
    def clean_up(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
        self.conn2.close()
