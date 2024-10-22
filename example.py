import csv
import os, time
import streamlit as st
from langchain_community.document_loaders import WebBaseLoader
from langchain_community.vectorstores import Chroma
from langchain_community import embeddings
from langchain_community.llms import Ollama
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain.text_splitter import CharacterTextSplitter


"""
This script showcases how we can use RAG on a csv file and deploy it locally using streamlit
"""
 # URL processing
def process_input(path_to_csv, question):
    model_local = Ollama(model="llama3.1:8b")
    
    # Open the CSV and store in a list
    with open(path_to_csv) as file:
        reader = csv.reader(file)
        rows = list(reader)
    
    # Normalize the user question to replace punctuation and make lowercase
    normalized_message = question.lower().replace("?", "").replace("(", " ").replace(")", " ")

    # Search the CSV for user question using very naive search
    words = normalized_message.split()
    matches = []
    for row in rows[1:]:
        # if the word matches any word in row, add the row to the matches
        if any(word in row[0].lower().split() for word in words) or any(word in row[3].lower().split() for word in words):
            matches.append(row)

    # Format as a markdown table, since language models understand markdown
    matches_table = " | ".join(rows[0]) + "\n" + " | ".join(" --- " for _ in range(len(rows[0]))) + "\n"
    matches_table += "\n".join(" | ".join(row) for row in matches)
    print(f"Found {len(matches)} matches:")
    print(matches_table)

    # Now we can use the matches to generate a response
    SYSTEM_MESSAGE = """
    You are a helpful assistant that answers questions about cars based off a car data set.
    You must use the data set provided after source: to answer the questions, you should not provide any info that is not in the provided sources.
    """

    qa_prompt = ChatPromptTemplate.from_messages([
                ("system", SYSTEM_MESSAGE),
                ("human", "{input}")
            ])
    q_chain = qa_prompt | model_local

    response = q_chain.invoke(
                {"input": question + "\nSources: " + matches_table})
    print("Response:", response)

    return response

    

st.title("RAG Query with Ollama")

# Initialize session state for storing conversation history
if 'conversation' not in st.session_state:
    st.session_state.conversation = []

# Input fields
USER_MESSAGE = st.text_input("RAG Query")

# load data
path_to_csv = os.path.join('Datasets','Cars.csv')

# Button to process input
if st.button('Send'):
    with st.spinner('Processing...'):
        answer = process_input(path_to_csv, USER_MESSAGE)

