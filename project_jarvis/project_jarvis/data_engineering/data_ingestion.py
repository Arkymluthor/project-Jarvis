import argparse
import asyncio
from typing import Any, Optional, Union
import glob
import os
from dotenv import load_dotenv

from azure.core.credentials import AzureKeyCredential


from project_autodesk.data_engineering.preprocess.blobmanager import BlobManager
from project_autodesk.data_engineering.preprocess.embeddings import (
    AzureOpenAIEmbeddingService,
    OpenAIEmbeddings,
    OpenAIEmbeddingService,
)
from project_autodesk.data_engineering.preprocess.fileprocessor import FileProcessor
from project_autodesk.data_engineering.preprocess.filestrategy import FileStrategy
from project_autodesk.data_engineering.preprocess.htmlparser import LocalHTMLParser, UnstructuredHTMLParser
from project_autodesk.data_engineering.preprocess.integratedvectorizerstrategy import (
    IntegratedVectorizerStrategy,
)
from project_autodesk.data_engineering.preprocess.listfilestrategy import (
    ListFileStrategy,
    LocalListFileStrategy,
)
from project_autodesk.data_engineering.preprocess.parser import Parser
from project_autodesk.data_engineering.preprocess.strategy import DocumentAction, SearchInfo, Strategy
from project_autodesk.data_engineering.preprocess.textparser import TextParser
from project_autodesk.data_engineering.preprocess.textsplitter import SentenceTextSplitter, SimpleTextSplitter

load_dotenv(override=True)


#key environment variables
storagekey = os.environ.get("AZURE_STORAGE_KEY")
storageaccount = os.environ.get("AZURE_STORAGE_NAME")
container = os.environ.get("AZURE_CONTAINER_NAME")


#Azure AI search
searchanalyzername = "en.microsoft"
searchkey = os.environ.get("AZURE_COGNITIVE_SEARCH_KEY")
searchservice=os.environ.get("AZURE_SEARCH_SERVICE_NAME")
search_index = os.environ.get("AZURE_SEARCH_INDEX_NAME")



#OpenAI Credentials
open_ai_service=os.environ.get("OPENAI_API_ENDPOINT")
open_ai_key=os.environ.get("OPEN_AI_KEY")
open_ai_deployment=os.environ.get("OPEN_AI_EMBEDDING_DEPLOYMENT_NAME")
open_ai_model_name=os.environ.get("OPEN_AI_EMBEDDING_MODEL_NAME")
open_ai_host=os.environ.get("OPEN_AI_HOST")


category = os.environ.get("EXTRACTION_CATEGORY", "sample category" )  #This can be used to bucket categories for information.


print("62")


async def setup_file_strategy(files:str, use_vectors:bool, verbose:bool=True, document_action=DocumentAction.Add) -> Strategy:

    """Setups a strategy for loading a file or html process into a document store ; Azure Cognitive / Azure AI Search"""
    blob_manager = BlobManager(
        endpoint=f"https://{storageaccount}.blob.core.windows.net",
        container=container,
        account=storageaccount,
        credential=storagekey,
        verbose=verbose,
    )

    html_parser: Parser
    html_parser = UnstructuredHTMLParser(verbose=verbose) 
    sentence_text_splitter = SentenceTextSplitter()
    file_processors = {
        ".html": FileProcessor(html_parser, sentence_text_splitter),
        ".md": FileProcessor(TextParser(), sentence_text_splitter),
        ".txt": FileProcessor(TextParser(), sentence_text_splitter),
    }

    embeddings: Optional[OpenAIEmbeddings] = None
    if use_vectors and open_ai_host != "openai":
        azure_open_ai_credential= open_ai_key
        embeddings = AzureOpenAIEmbeddingService(
            open_ai_service=open_ai_service,
            open_ai_deployment=open_ai_deployment,
            open_ai_model_name=open_ai_model_name,
            credential=azure_open_ai_credential,
            verbose=verbose,
        )
    elif use_vectors:
        embeddings = OpenAIEmbeddingService(
            open_ai_model_name=open_ai_model_name,
            credential=open_ai_key,
            organization=open_ai_host,
            verbose=verbose,
        )


    print("Processing files...")
    list_file_strategy: ListFileStrategy
    print(f"Using local files in {files}")
    list_file_strategy = LocalListFileStrategy(path_pattern=files, verbose=verbose)

    if document_action == DocumentAction.RemoveAll:
        task = document_action
    elif document_action == DocumentAction.Remove:
        task = document_action
    else:
        task = document_action

    return FileStrategy(
        list_file_strategy=list_file_strategy,
        blob_manager=blob_manager,
        file_processors=file_processors,
        document_action=task,
        embeddings=embeddings,
        search_analyzer_name=searchanalyzername,
        category=category,
    )


async def setup_intvectorizer_strategy(files:str,use_vectors:bool,verbose:bool=True,document_action=DocumentAction.Add) -> Strategy:
    
    """Setups a strategy for loading a file or html process into a document store ; Azure Cognitive / Azure AI Search & In built function skill for embeddings"""
    blob_manager = BlobManager(
        endpoint=f"https://{storageaccount}.blob.core.windows.net",
        container=container,
        account=storageaccount,
        credential=storagekey,
        verbose=verbose,
    )

    embeddings: Union[AzureOpenAIEmbeddingService, None] = None
    if use_vectors and open_ai_host != "openai":
        azure_open_ai_credential= open_ai_key

        embeddings = AzureOpenAIEmbeddingService(
            open_ai_service=open_ai_service,
            open_ai_deployment=open_ai_deployment,
            open_ai_model_name=open_ai_model_name,
            credential=azure_open_ai_credential,
            verbose=verbose,
        )

    elif use_vectors:
        embeddings = OpenAIEmbeddingService(
            open_ai_model_name=open_ai_model_name,
            credential=open_ai_key,
            organization=open_ai_host,
            verbose=verbose,
        )

    print("Processing files...")
    print(f"Using local files in {files}")
    list_file_strategy = LocalListFileStrategy(path_pattern=files, verbose=verbose)


    return IntegratedVectorizerStrategy(
        list_file_strategy=list_file_strategy,
        blob_manager=blob_manager,
        document_action=document_action,
        embeddings=embeddings,
        search_analyzer_name=searchanalyzername,
        category=category,
    )


async def main(files:str,document_action=DocumentAction.Add,use_int_vectorization:bool=False,use_vectors:bool=False, verbose:bool=True):

    if use_int_vectorization:
        strategy = await setup_intvectorizer_strategy(files,use_vectors,verbose,document_action)
    else:
        strategy = await setup_file_strategy(files, use_vectors, verbose, document_action)

    search_info = SearchInfo(
        endpoint=f"https://{searchservice}.search.windows.net/",
        credential=searchkey,
        index_name=search_index,
        verbose=verbose,
    )

    print("185")
    
    if (document_action != DocumentAction.RemoveAll) and (document_action != DocumentAction.Remove):
        await strategy.setup(search_info)

    print("190")

    await strategy.run(search_info)


if __name__ == "__main__":

    #Strategy details to use

    files="../pages"  #path  to html stores
    document_action = DocumentAction.Add    #Document addition to blob storage
    use_int_vectorization = False  #use integrated vectorization as skill or embed on the go
    use_vectors = True  #Create embeddings 
    verbose=True


    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(files,document_action,use_int_vectorization, use_vectors,verbose))
