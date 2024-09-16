import os
import numpy as np
import pandas as pd
from pg_conn import PG
from populate import Schema
from pgvector.psycopg2 import register_vector
from langchain_ollama import OllamaEmbeddings

class RAG:
    """
    A class which is going to be a buildng block and the first step to understand RAG

    This contains creation of schema, population and getting embeddings using Ollama model and storing it as well in postgresql.
    Then we can use pgvector to get the nearest distance from the question embeddings to the stored embeddings.
    """

    def __init__(self, dbname) -> None:
        self.pg = PG(dbname)
        # self.embeddings = OllamaEmbeddings(
        #     model="llama3.1:8b",
        # )

    def get_table_ready(self, dbname, table_name, path_to_csv, embedd_col):
        populate = Schema(dbname)
        dtype_mapping = {
        'int64': 'integer',
        'float64': 'numeric',
        'object': 'text',
        'datetime64[ns]': 'timestamp',
        'bool': 'boolean'
        }
        df = pd.read_csv(path_to_csv)
        df['id'] = range(1 , 1 + len(df))
        df = df[['id'] + [col for col in df.columns if col != 'id']]
        df.columns = [item.strip().replace(' ','_').lower() for item in df.columns.to_list()]
        # get vector embeddings for description
        # df['embedding'] = None
        # for i in range(len(df)):
        #     df['embedding'][i] = self.embeddings.embed_query(df[embedd_col][i])[:256]
        column_dict = {}
        for i, (column, dtype) in enumerate(df.dtypes.items()):
            if i == 0:  # Check if it's the first column
                column_dict[column] = 'integer PRIMARY KEY'
            else:
                sql_dtype = dtype_mapping.get(str(dtype), 'text')  # Default to 'text' if dtype is not found
                column_dict[column] = sql_dtype
        if self.pg.table_exists(table_name):
            self.pg.cursor.execute(f"DROP TABLE {table_name}")
            self.pg.conn.commit()
            print(f"Table {table_name} dropped.")
        else:
            print(f"Table {table_name} does not exist.")
        populate.build_table(table_name, column_dict)
        populate.populate_table_from_csv(table_name, df)
        populate.clean_up()
        print()

    def query_rag(self, table_name, col_name, embed_col_name):
        self.pg.execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
        register_vector(self.pg.conn)
        self.pg.execute_sql(f"CREATE INDEX ON {table_name} USING hnsw (embedding vector_l2_ops)")
        print()

        # Get question from user
        question = "which is the Premium Package 4dr Sedan (2.4L 4cyl 8AM)"

        # Turn the question into an embedding
        client = EmbeddingsClient(endpoint=endpoint, credential=AzureKeyCredential(token))
        response = client.embed(input=question, model=model_name, dimensions=256)
        embedding = np.array(response.data[0].embedding)

        # Perform the database query
        self.pg.cursor.execute(
            f"""
            WITH semantic_search AS (
                SELECT id, RANK () OVER (ORDER BY embedding <=> %(embedding)s) AS rank
                FROM {table_name}
                ORDER BY embedding <=> %(embedding)s
                LIMIT 20
            ),
            keyword_search AS (
                SELECT id, RANK () OVER (ORDER BY ts_rank_cd(to_tsvector('english', {col_name} || ' ' || {embed_col_name}), query) DESC)
                FROM {table_name}, plainto_tsquery('english', %(query)s) query
                WHERE to_tsvector('english', {col_name} || ' ' || {embed_col_name}) @@ query
                ORDER BY ts_rank_cd(to_tsvector('english', {col_name} || ' ' || {embed_col_name}), query) DESC
                LIMIT 20
            )
            SELECT
                COALESCE(semantic_search.id, keyword_search.id) AS id,
                COALESCE(1.0 / (%(k)s + semantic_search.rank), 0.0) +
                COALESCE(1.0 / (%(k)s + keyword_search.rank), 0.0) AS score
            FROM semantic_search
            FULL OUTER JOIN keyword_search ON semantic_search.id = keyword_search.id
            ORDER BY score DESC
            LIMIT 20
            """,
            {"query": question, "embedding": embedding, "k": 60},
        )

        results = self.pg.cursor.fetchall()

        # Fetch the {table_name} by ID
        ids = [result[0] for result in results]
        self.pg.cursor.execute(f"SELECT id, {col_name}, {embed_col_name} FROM {table_name} WHERE id = ANY(%s)", (ids,))
        results = self.pg.cursor.fetchall()

        # Format the results for the LLM
        formatted_results = ""
        for result in results:
            formatted_results += f"## {result[1]}\n\n{result[2]}\n"

        # Generate the response using Ollama
        response = client.complete(
            messages=[
                {"role": "system", "content": f"You must answer user question according to sources. Say you dont know if you cant find answer in sources. Cite the {col_name} name of each {table_name} square brackets. The title of each {table_name} which will be a markdown heading."},
                {"role": "user", "content": question + "\n\nSources:\n\n" + formatted_results},
            ],
            model="ollama-gpt-4",
            temperature=0.3,
            max_tokens=1000
        )

        print("Answer:\n\n")
        print(response.choices[0].message.content)



if __name__=='__main__':
    dbname = 'RagHack'
    obj = RAG(dbname)
    path_to_csv = os.path.join('Datasets','Cars.csv')
    table_name = 'cars'
    # this is the column name of which you want embeddings
    embed_col = 'trim_description'
    need_to_populate = True
    if need_to_populate:
        obj.get_table_ready(dbname, table_name,path_to_csv, embed_col)
    obj.query_rag(table_name)

