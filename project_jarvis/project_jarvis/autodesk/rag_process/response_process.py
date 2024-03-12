import asyncio
from operator import itemgetter
from langchain_community.vectorstores import FAISS
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda, RunnablePassthrough
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_core.messages import AIMessage, HumanMessage, get_buffer_string
from langchain_core.prompts import format_document
from langchain_core.runnables import RunnableParallel
from langchain.prompts.prompt import PromptTemplate
from langchain.docstore.document import Document
from typing import Any, AsyncGenerator, Awaitable, Callable, List, Optional, Union, cast
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langchain.memory import ConversationBufferMemory

import os
import json


from project_autodesk.autodesk.rag_process.llm_clients import azure_openai_client
from project_autodesk.autodesk.rag_process.retrieval_clients import azure_document_retriever,AzureUrlClient



#AI Assistant run context

context = {"overrides":{"retrieval_mode": "hybrid",
                        "semantic_ranker":True,
                        "semantic_captions":True,
                        "top":3}}
    


#Prompt templates for single and sustained conversations.

template = """
You are an assistant that provides answers based on only available information.

You will get context in json format.

Each header is a url of the source. 

{context}

All answers have to be based on facts that can be verified.

Tell the user you do not know if you do not find an answer and You can choose to share the company home page with them "https://www.autodesk.com/" if you consider it relevant to the question asked.

You are allowed to respond to pleasantries.

Limit all answers to a maximum of two hundred words only (200 words)

Response with answer and sources in a python dictionary with answer and sources as keys.

Never Markdown in response. 

Question: {question}

Answer:

"""



prompt = ChatPromptTemplate.from_template(template)


_template = """Given the following conversation and a follow up question, rephrase the follow up question to be a standalone question, in its original language.

Chat History:
{chat_history}
Follow Up Input: {question}
Standalone question:"""
CONDENSE_QUESTION_PROMPT = PromptTemplate.from_template(_template)



def extract_dictionary(json_string):
     # Parse the JSON string into a Python dictionary

    try:
       
        dictionary = json.loads(json_string)
        return dictionary
    except json.JSONDecodeError:
        print("Error: Invalid JSON string")
        return json_string



def format_docs(documents):
    """Creates a dict format for content and sources"""
    retriever = {}
    if len(documents)>0:
        for doc in documents:
            url = AzureUrlClient(doc.metadata["sourcepage"])
            retriever[url] = doc.page_content
    
    print(retriever)

    return retriever



memory = ConversationBufferMemory(return_messages=True, output_key="answer", input_key="question")

async def conversational_llm_response_with_memory(user_query:str,memory,llm_client:ChatOpenAI=azure_openai_client,prompt:ChatPromptTemplate=prompt,retriever=azure_document_retriever):
        
        """ Function to process user request, provide response and maintaining conversation history"""
        

        try:
            if len(user_query) > 0:
                documents = await azure_document_retriever.run(user_query,context=context)
                retriever = format_docs(documents)
            else:
                return "Please provide some context"
        except Exception as error:
             print(error)
             return "Error in document retrieval."
        

        try:

            loaded_memory = RunnablePassthrough.assign(
                chat_history=RunnableLambda(memory.load_memory_variables) | itemgetter("history"),
            )

            _inputs = {
                 "standalone_question":{
                      "question": lambda func: func["question"],
                      "chat_history":lambda x: get_buffer_string(x["chat_history"])
            }
            | CONDENSE_QUESTION_PROMPT
            | llm_client
            | StrOutputParser(),}


            _context_docs = {
            "docs": lambda func: retriever,
            "question": lambda x: x["standalone_question"]}

            _final_inputs = {
            "context": lambda func: retriever,
            "question": itemgetter("question")}

            # And finally, we do the part that returns the answers
            answer = {
                "answer": _final_inputs | prompt |llm_client,
                "docs": itemgetter("docs"),
            }

            # And now we put it all together!
            final_chain = loaded_memory | _inputs | _context_docs | answer

            result = await final_chain.ainvoke({"question":user_query})

            #Update memory

            memory.save_context({"question":user_query}, {"answer": result["answer"].content})


        except Exception as error:
             print(error)
             return "Unable to assist with the information you need, please try again later."
        

        return extract_dictionary(result['answer'].content)




async def llm_response(user_query:str,llm_client:ChatOpenAI=azure_openai_client,prompt:ChatPromptTemplate=prompt,retriever=azure_document_retriever):
        
         
        """ Function to process user request, provide response. Usefull for the RAG evaluation"""
        
        
        try:
            if len(user_query) > 0:
                documents = await azure_document_retriever.run(user_query,context=context)
                retriever = format_docs(documents)
            else:
                return "Please provide some context"
        except Exception as error:
             print(error)
             return "Error in document retrieval."

        try:
     

            chain = ({"context":lambda func: retriever ,
                    "question": RunnablePassthrough()}
                    | prompt
                    | llm_client
                    | StrOutputParser())
            
            result = await chain.ainvoke(user_query)

        except Exception as error:
             print(error)
             return "Unable to assist with the information you need, please try again later."
        

        return extract_dictionary(result)






