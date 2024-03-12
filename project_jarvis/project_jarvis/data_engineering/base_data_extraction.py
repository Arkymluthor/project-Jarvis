#base libraries
import glob
import logging
import pandas as pd
import numpy as np 
import os
from typing import Dict, List
import json
from pathlib import Path

#extraction libraries
from langchain_community.document_loaders import BSHTMLLoader, UnstructuredHTMLLoader
from langchain_community.document_transformers import BeautifulSoupTransformer, Html2TextTransformer
from langchain.docstore.document import Document
from unstructured.cleaners.core import replace_unicode_quotes, clean, group_broken_paragraphs


def load_files(folder_path:str) -> List:
    '''Loads files from path given or return empy list if none'''
    try:
        
        files = glob.glob(f"{folder_path}*.html")

    except Exception as error:

        logging.info("Files could not be loaded, check path")
        files = []

    return files


def parse_html_to_doc(file_path:str, include_page_breaks:bool=True) -> List:

    unstructured_kwargs = {"include_page_breaks":include_page_breaks}

    document = []

    state = False

    try:
        unstructed_loader = UnstructuredHTMLLoader(file_path,**unstructured_kwargs)
        document = unstructed_loader.load()
        state= True

    except Exception as error:
        logging.error(error)
        logging.info(f"Unable to complete process for this file: {file_path}")
        pass

    return document, state



def cleanup_document(Document):
    '''Look to remove footers and other unnecessary informations'''
    pass


def create_child_documents(parent_document:Document):
    '''Create a json store of all child documents'''

    pass

def store_documents(file_name:str,output_folder:str, parent_document:List):
    '''Create a json store of all mother documents'''

    empty = 0

    try:
        json_file_name = Path(file_name).stem
        if len(parent_document) > 1:
            for i in len(parent_document):
                doc = {}
                    
                doc["page content"] =  parent_document[i].page_content

                if len(parent_document[i].page_content) < 5:
                    doc['status'] = "Failed"
                    empty +=1

                else:
                    doc['status'] = "Passed"


                doc["metadata"] =  parent_document[i].metadata

                doc["node"] = "parent"

                with open(f"{output_folder}/{json_file_name}_#{i+1}.json",'w') as file:
                    json.dump(doc,file,indent=4)
                    file.close()

        else:
            doc = {}
                    
            doc["page_content"] =  parent_document[0].page_content

            doc["metadata"] =  parent_document[0].metadata

            doc["node"] = "parent"

            if len(parent_document[0].page_content) < 5:
                doc['status'] = "Failed"
                empty += 1

            else:
                doc['status'] = "Passed"


            with open(f"{output_folder}/{json_file_name}.json",'w') as file:
                json.dump(doc,file,indent=4)
                file.close()

        

    except Exception as error:
        logging.error(error)
        
    return empty



def process_extraction(folder_path:str,output_folder:str="json_data", store_docs:bool=None):
    '''Run the data extraction process'''

    files_to_extract = load_files(folder_path) #list of paths

    

    Path(output_folder).mkdir(parents=True,exist_ok=True)

    successful =0
    docs_empty = 0
    for file in files_to_extract:
        doc,state = parse_html_to_doc(file)
        if state: #extract each path
            successful+=1
            logging.info(f"Parsed html {file}")
            empty_docs = store_documents(file,output_folder,doc) #store objects
            docs_empty += empty_docs
            logging.info(f"Created a json object for {file}")

    print(f"Successfully extracted {successful}/{len(files_to_extract)}")
    print(f"Observed {docs_empty} htmls with no page content")
    logging.info("Done")

    
if __name__ == "__main__":

    folder_path = "../pages/"

    process_extraction(folder_path)















    





        
        


