from pg_conn import PG

class Schema:

    def __init__(self,dbname) -> None:
        self.pg = PG(dbname)

    def build_table(self, table_name, columns):
        # Construct the columns part of the SQL statement
        columns_sql = ",\n".join([f"{col_name} {data_type}" for col_name, data_type in columns.items()])
        
        # Create the SQL statement
        create_table_sql = f"""CREATE TABLE {table_name} (
            {columns_sql}
        );"""
        
        # Execute the SQL statement
        self.pg.execute_sql(create_table_sql)
        print(f"Created {table_name}")
        self.pg.conn.commit()

    def populate_table_from_csv(self, table_name, df):
        csv_columns = set(df.columns)
        table_columns = self.pg.get_table_columns(table_name)
        # Check if columns match
        if csv_columns == table_columns:
            # Insert data into the table
            self.pg.to_psql(df, table_name)
            print(f"Data from  has been inserted into {table_name}")
        else:
            print("Column mismatch between CSV and table schema.")
            print(f"CSV columns: {csv_columns}")
            print(f"Table columns: {table_columns}")

    def clean_up(self):
        self.pg.clean_up()

