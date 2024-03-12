# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
import sys 
from botbuilder.core import (
    ActivityHandler,
    TurnContext,
    UserState,
    CardFactory,
    MessageFactory,
)
from botbuilder.schema import (
    ChannelAccount,
    HeroCard,
    CardImage,
    CardAction,
    ActionTypes,
    ActivityTypes,
    Activity,
    Attachment,
)

from langchain.memory import ConversationBufferMemory

from rag_process.response_process import llm_response,conversational_llm_response_with_memory
from bots.utils import create_adaptive_card

class AutodeskBot(ActivityHandler):

    def __init__(self):
        self.memory = {}





    async def on_members_added_activity(
        self, members_added: [ChannelAccount], turn_context: TurnContext
    ):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Welcome, I am Jarvis, your information assistant. How can I help you today?")


    async def on_message_activity(self, turn_context: TurnContext):

        conversation_id = turn_context.activity.conversation.id
        user_query = turn_context.activity.text.strip()
        reply = Activity(type=ActivityTypes.message)
        #remove all trailing spaces

        if len(user_query) == 0:

            return await turn_context.send_activity(MessageFactory.text("Hi, how can I help you today?"))
        
        if type(user_query) is not str:

            return await turn_context.send_activity(MessageFactory.text("Hi, can you ask the question again"))
        

        if len(user_query.split()) > 200:
            return await turn_context.send_activity(MessageFactory.text("Hi, rephrase the question to be concise."))
        

        #check and add a conversation memory
        conversation_memory =  self.memory.get(conversation_id,None)

        if conversation_memory is None:

            conversation_memory = ConversationBufferMemory(return_messages=True, output_key="answer", input_key="question", k=5)
            self.memory[conversation_id] = conversation_memory
        
        try:
            gen_response =  await self.completionAPIHandler(user_query,memory=conversation_memory )

            if type(gen_response) is dict:
                reply.text = gen_response.get('answer',"No information found")
                if len(reply.text) == 0:
                    reply.text = "I am unable to process any information right now. Please let's try again later."

                if ("sources" in gen_response) and (len(gen_response["sources"].split(","))>0):
                    reply.attachments = [self._get_inline_attachement(src) for src in gen_response["sources"].split(",")]
            else:
                if len(gen_response) > 0:
                    reply.text= gen_response
                else:
                    reply.text = "I am unable to process any information right now. Please let's try again later."


            
            return await turn_context.send_activity(reply)
        except Exception as error:
            reply.text = f"{sys.exc_info()}"

    
    async def completionAPIHandler(self, user_query,memory):
        """Accepts the user query and makes a document retrieval and completion"""
        try:
            completion = await conversational_llm_response_with_memory(user_query,memory)

            print("Completion",completion)

        except Exception as error:

            completion = f"Unable to complete due to {sys.exc_info()}"

        return completion
    
    def _get_inline_attachement(self,url) -> Attachment:

        cotent = create_adaptive_card(url)

        return CardFactory.adaptive_card(cotent)
