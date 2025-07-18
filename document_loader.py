import os
from langchain_community.document_loaders import UnstructuredExcelLoader,Docx2txtLoader,PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import AzureBlobStorageContainerLoader
import pandas as pd
from dotenv import load_dotenv
# load_dotenv(
#     dotenv_path='lib/.env.local',override=True
# )

def fetch_file(file):        
        temp_dir = "temp"
        os.makedirs(temp_dir, exist_ok=True)
        
        with open(os.path.join(temp_dir, file.name), "wb") as f:
                                
                f.write(file.getbuffer()) 
                file_path = os.path.join(temp_dir, file.name)
                # print(file_path)
                # print(file.name,"written to temp")
        
        return file_path
        
#embeddings = OllamaEmbeddings(model="nomic-embed-text")
def load_split_pdf(path,chunking):
        loader = PyPDFLoader(path)
        documents = loader.load()
        if chunking == True:
            text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000,chunk_overlap=200)
            all_splits = text_splitter.split_documents(documents) 
            return documents,all_splits 
        else:
              return documents
def load_excel(path,chunking):
        loader = UnstructuredExcelLoader(path)
        documents = loader.load()
        if chunking == True:
            text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000,chunk_overlap=200)
            all_splits = text_splitter.split_documents(documents) 
            print("all_splits",all_splits)
            return documents,all_splits 
        else:
              return documents 
        
def load_docx(path,chunking):
        
        loader=Docx2txtLoader(path)
        documents = loader.load()
        if chunking == True:
            text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000,chunk_overlap=200)
            all_splits = text_splitter.split_documents(documents) 
            return documents,all_splits 
        else:
            return documents
# def load_from_azureblob():
#         azure_storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
#         azure_storage_account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
#         azure_storage_conn_str = f"DefaultEndpointsProtocol=https;AccountName={azure_storage_account_name};AccountKey={azure_storage_account_key};EndpointSuffix=core.windows.net"
#         container_name = "documents"  # Replace with your actual container name
#         loader = AzureBlobStorageContainerLoader(
#         conn_str=azure_storage_conn_str,
#         container=container_name,
#         prefix="Data_Engineering_Best_Practices.pdf"
#         )
#         documents = loader.load()
#         return(documents[0].page_content)