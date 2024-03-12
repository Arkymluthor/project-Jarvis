import os
import asyncio
from azure.search.documents.aio import SearchClient
from openai import AzureOpenAI, AsyncOpenAI, AsyncAzureOpenAI
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob.aio import BlobServiceClient, ContainerClient
from dotenv import load_dotenv


from project_autodesk.autodesk.rag_process.azure_document_search import AzureAISearchClient

load_dotenv(override=True)


#Azure AI search
searchkey = os.environ.get("AZURE_COGNITIVE_SEARCH_KEY")
searchservice=os.environ.get("AZURE_SEARCH_SERVICE_NAME")
search_index = os.environ.get("AZURE_SEARCH_INDEX_NAME")


#OpenAI Credentials
open_ai_service=os.environ.get("OPENAI_API_ENDPOINT")
open_ai_key=os.environ.get("OPEN_AI_KEY")
open_ai_deployment=os.environ.get("OPEN_AI_EMBEDDING_DEPLOYMENT_NAME")
open_ai_model_name=os.environ.get("OPEN_AI_EMBEDDING_MODEL_NAME")
open_ai_host=os.environ.get("OPEN_AI_HOST")


#LinkClient
storageaccount = os.environ.get("AZURE_STORAGE_NAME")
container = os.environ.get("AZURE_CONTAINER_NAME")


search_client = SearchClient(endpoint=f"https://{searchservice}.search.windows.net/",
        credential=AzureKeyCredential(searchkey),
        index_name=search_index)


openai_client = AsyncAzureOpenAI(azure_endpoint=f"https://{open_ai_service}.openai.azure.com",
            azure_deployment = open_ai_deployment,
             api_version =  "2023-03-15-preview",
              api_key = open_ai_key)


azure_document_retriever =  AzureAISearchClient(search_client=search_client,
                                                openai_client= openai_client,
                                                embedding_deployment=open_ai_deployment,
                                                embedding_model=open_ai_model_name,
                                                openai_host=open_ai_host)




def AzureUrlClient(source:str,storageaccount:str=storageaccount,container:str=container):
        """Reconstructures the storage path for stored htmls"""
        endpoint=f"https://{storageaccount}.blob.core.windows.net"
        
        url = f"{endpoint}/{container}/{source}"
        return url








# if __name__ == "__main__":

#     query_text = "I would like to speak with a Fusion 360 expert"

#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     results = loop.run_until_complete(azure_document_retriever.run(query_text))

#     for r in results:
#         print(r.page_content)




