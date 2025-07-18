import os
from langchain_openai import AzureOpenAIEmbeddings
import pandas as pd
from dotenv import load_dotenv
from azure.cosmos import CosmosClient
from azure.cosmos.partition_key import PartitionKey
from langchain_community.vectorstores.azure_cosmos_db_no_sql import (
    AzureCosmosDBNoSqlVectorSearch,
)
from langchain_openai import AzureOpenAIEmbeddings
from sklearn.metrics.pairwise import cosine_similarity
from langchain.schema import Document
import numpy as np
from azure.identity import DefaultAzureCredential
load_dotenv(
    dotenv_path='.env.local',override=True
)
os.environ["AZURE_OPENAI_API_KEY"] = os.environ["OPENAI_API_KEY"]



embeddings = AzureOpenAIEmbeddings(
    azure_endpoint=os.environ["OPENAI_API_ENDPOINT"],
    azure_deployment="text-embedding-3-small",
    openai_api_version=os.environ["OPENAI_API_VERSION"],
    model="text-embedding-3-small",
)
cosmos_client = CosmosClient(
    url=os.environ["COSMOS_DB_URL"],
    #credential=DefaultAzureCredential(),
    credential=os.environ["COSMOS_DB_KEY"],
    
)
database = cosmos_client.get_database_client('Codecreation-RAG')
container = database.get_container_client('domain_doc')
cosmos_container_properties={"partition_key":PartitionKey(path="/id")}
class CosmosDBNoSQLRetriever:
    def __init__(self, documents):
        self.documents = documents

    def similarity_search(self, query, k=5):
        query_embedding = embeddings.embed_query(query)
        results = []

        for doc in self.documents:
            doc_embedding = np.array(doc['embedding'])
            score = cosine_similarity([query_embedding], [doc_embedding])[0][0]
            results.append((doc, score))

        results.sort(key=lambda x: x[1], reverse=True)
        return [
            Document(page_content=doc['text'], metadata=doc['metadata'])
            for doc, _ in results[:k]
        ]
def delete_container():
    # for container in database.list_containers():
    #     cosmos_container = container['id']
    #     print(f"Deleting container: {cosmos_container}")
    #     database.delete_container(cosmos_container)
    # print("All containers deleted.")
    items_to_delete = list(container.query_items(
	query="SELECT c.id FROM c",
	enable_cross_partition_query=True))
    # Delete each item individually, using its id as the partition key
    for item in items_to_delete:
        container.delete_item(item=item['id'], partition_key=item['id'])
    print("All items deleted from the container.")
def create_vectorstore(all_splits,cosmos_container):
    vector_embedding_policy = {'vectorEmbeddings': [{'path': '/embedding', 'dataType': 'float32', 'dimensions': 1536, 'distanceFunction': 'cosine'}]}
    indexing_policy = {'indexingMode': 'consistent', 'automatic': True, 'includedPaths': [{'path': '/*'}], 'excludedPaths': [{'path': '/"_etag"/?'}, {'path': '/embedding/*'}], 'fullTextIndexes': [], 'vectorIndexes': [{'path': '/embedding', 'type': 'quantizedFlat', 'quantizationByteSize': 96}]}    
    # cosmos_container = database.create_container_if_not_exists(id=cosmos_container,
    # partition_key=PartitionKey(path="/id"),
    # indexing_policy=indexing_policy,
    # vector_embedding_policy=vector_embedding_policy,
    # )
    database="Codecreation-RAG"
    #cosmos_container = 'domain_doc'
    vector_store = AzureCosmosDBNoSqlVectorSearch.from_documents(
    documents=all_splits,
    embedding=embeddings,
    cosmos_client=cosmos_client,
    database_name=database,
    container_name=cosmos_container,
    vector_embedding_policy=vector_embedding_policy,
    indexing_policy=indexing_policy,
    cosmos_database_properties={},
    cosmos_container_properties=cosmos_container_properties,
    create_container=False,
    )
#    vector_store.add_documents(all_splits)
    return vector_store
def search_vectorstore(vectorsearch_keywords):
    for container in database.list_containers():
        cosmos_container=container['id']
    cosmos_container=database.get_container_client(cosmos_container)
    docs = cosmos_container.query_items(
        query="SELECT * FROM c",
        #partition_key=PartitionKey(path="/id"),
        enable_cross_partition_query=True,
    )
    retriever = CosmosDBNoSQLRetriever(docs)
    results = retriever.similarity_search(vectorsearch_keywords, k=5)
    return results