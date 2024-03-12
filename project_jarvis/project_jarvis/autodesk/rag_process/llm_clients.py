import os

from openai import AzureOpenAI
from langchain_openai import AzureOpenAIEmbeddings
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

from dotenv import load_dotenv

load_dotenv(override=True)

#OpenAI Credentials
open_ai_service=os.environ.get("OPENAI_API_ENDPOINT")
open_ai_key=os.environ.get("OPEN_AI_KEY")
open_ai_deployment=os.environ.get("OPENAI_AI_DEPLOYMENT_NAME")
open_ai_model_name=os.environ.get("OPEN_AI_MODEL_NAME")
open_ai_host=os.environ.get("OPEN_AI_HOST")



azure_openai_client = AzureChatOpenAI(azure_endpoint = f"https://{open_ai_service}.openai.azure.com",
                      azure_deployment =open_ai_deployment,
                      model=open_ai_model_name,
                      api_version =  "2023-03-15-preview",
                      api_key = open_ai_key)