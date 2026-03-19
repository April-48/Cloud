import json
import boto3
import uuid  # Used for generating unique session IDs when needed

# Initialize Lex V2 client for AWS chatbot service
client = boto3.client('lexv2-runtime')


def lambda_handler(event, context):
    print("Event received:", json.dumps(event))

    # Bot configuration (you can find these in your AWS Lex console)
    BOT_ID = 'E3BOOT4XUS'  # Your Bot ID
    BOT_ALIAS_ID = 'TSTALIASID'  # Your Bot Alias ID
    LOCALE_ID = 'en_US'

    # Parse the incoming message from the frontend
    try:
        if 'body' not in event or event['body'] is None:
            raise ValueError("Body is missing")

        body = json.loads(event['body'])

        # Extract the user's message from the request
        if 'messages' in body:
            user_message = body['messages'][0]['unstructured']['text']
        else:
            user_message = body.get('message', 'Hello')

        # Dynamically retrieve the session ID
        # Use userId from frontend if provided
        if 'userId' in body and body['userId']:
            SESSION_ID = body['userId']
            print(f"Using Frontend ID: {SESSION_ID}")
        else:
            # Generate a random UUID if frontend doesn't provide userId
            # This prevents errors and avoids hardcoding session IDs
            SESSION_ID = str(uuid.uuid4())
            print(f"Generated Random ID: {SESSION_ID}")

    except Exception as e:
        print("Error parsing input:", e)
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Invalid input format'})
        }

    # Send the user's message to Lex for processing
    try:
        response = client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=SESSION_ID,
            text=user_message
        )

        # Define a default fallback message (only shown when Lex returns no response)
        bot_response = "Sorry, I am a Dining Concierge. Please say 'I am hungry' to get started."

        # Use Lex response if available
        # If FallbackIntent is configured in Lex, it will override the default message
        if 'messages' in response and len(response['messages']) > 0:
            bot_response = response['messages'][0]['content']

    except Exception as e:
        print("Error calling Lex:", e)
        # Handle Lex errors with a friendly message
        bot_response = "Sorry, my brain is offline right now. Please try again later."
    # Return the response to the frontend
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps({
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "text": bot_response
                    }
                }
            ]
        })
    }