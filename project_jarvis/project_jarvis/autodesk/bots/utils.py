from botbuilder.core import (
    ActivityHandler,
    TurnContext,
    UserState,
    CardFactory,
    MessageFactory,
)
from botbuilder.schema import (
    HeroCard,
    CardImage,
    CardAction,
    ActionTypes,
    ActivityTypes,
)

def create_adaptive_card(url:str):
   content = {
    "type": "AdaptiveCard",
    "body": [
        {
            "type": "TextBlock",
            "size": "Small",
            "weight": "Bolder",
            "text": "Click below for more details."
        }],
    "actions": [
        {
            "type": "Action.OpenUrl",
            "title": "Click to view page",
            "url": f"{url}",
            "style":"positive"
        }
    ],
    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
    "version": "1.0"
    }
   
   return content
