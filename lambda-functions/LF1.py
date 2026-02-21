import json
import boto3
import logging
import time
import os

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Configuration Section (Please modify)
# 1. Your SQS queue URL (copy from SQS console)
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/851311377139/DiningQueue'

# 2. DynamoDB table name for storing user state
STATE_TABLE_NAME = 'user-state'

# 3. Validation rules (all lowercase)
# Define which cities and cuisines your bot supports
VALID_CITIES = ['manhattan']
VALID_CUISINES = [

    'chinese',

    'indian',

    'italian',

    'mexican',

    'japanese',

    'thai',  # Thai cuisine (many in Manhattan)

    'french',  # French cuisine

    'burgers',  # Burgers (American fast food)

    'pizza',  # Pizza (NYC specialty!)

    'korean',  # Korean cuisine (K-town)

    'mediterranean',  # Mediterranean cuisine

    'vietnamese'  # Vietnamese cuisine

]

# Initialize AWS clients
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
state_table = dynamodb.Table(STATE_TABLE_NAME)


# Helper functions (Lex standard response format)
def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']


def elicity_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    """Ask Lex to elicit a specific slot"""
    return {
        'sessionState': {
            'dialogAction': {'type': 'ElicitSlot', 'slotToElicit': slot_to_elicit},
            'intent': {'name': intent_name, 'slots': slots},
            'sessionAttributes': session_attributes
        },
        'messages': [{'contentType': 'PlainText', 'content': message}]
    }


def close(session_attributes, intent_name, fulfillment_state, message):
    """Close the conversation"""
    response = {
        'sessionState': {
            'dialogAction': {'type': 'Close'},
            'intent': {'name': intent_name, 'state': fulfillment_state},
            'sessionAttributes': session_attributes
        }
    }
    if message:
        response['messages'] = [{'contentType': 'PlainText', 'content': message}]
    return response


def delegate(session_attributes, slots, intent_name):
    """Delegate control to Lex to continue with the next slot"""
    return {
        'sessionState': {
            'dialogAction': {'type': 'Delegate'},
            'intent': {'name': intent_name, 'slots': slots},
            'sessionAttributes': session_attributes
        }
    }


# Core business logic

def validate_input(slots):
    """
    Validate user input for location and cuisine
    (Corresponds to Requirement 3.B & 3.C)
    """
    if not slots:
        return {'isValid': True}

    # Validate Location
    if 'Location' in slots and slots['Location'] and slots['Location']['value'].get('originalValue'):
        # Added .strip() to remove leading/trailing whitespace
        city = slots['Location']['value']['originalValue'].lower().strip()

        # Debug log to see what was received
        logger.info(f"Validating city: '{city}'")

        if city not in VALID_CITIES:
            return {
                'isValid': False,
                'violatedSlot': 'Location',
                'message': f"Sorry, I currently only serve the Manhattan area. I cannot fulfill requests for {city}. Please try 'Manhattan'."
            }

    # Validate Cuisine
    if 'Cuisine' in slots and slots['Cuisine'] and slots['Cuisine']['value'].get('originalValue'):
        # Added .strip() to remove leading/trailing whitespace
        cuisine = slots['Cuisine']['value']['originalValue'].lower().strip()

        if cuisine not in VALID_CUISINES:
            return {
                'isValid': False,
                'violatedSlot': 'Cuisine',
                'message': f"Sorry, I don't have suggestions for {cuisine}. Try Chinese, Japanese, Italian, mexican, burgers, pizza, vietnamese, thai, or french."
            }

    return {'isValid': True}


def save_user_state(user_id, data):
    """
    Save user's search preferences to DynamoDB
    (Corresponds to Extra Credit: Store State)
    """
    if not user_id:
        return
    try:
        # Add UserID and timestamp
        item = data.copy()
        item['userId'] = user_id
        item['timestamp'] = int(time.time())

        state_table.put_item(Item=item)
        logger.info(f"✅ State saved successfully for user: {user_id}")
    except Exception as e:
        logger.error(f"❌ Error saving state to DynamoDB: {e}")


def get_user_state(user_id):
    """
    Retrieve user's previous search preferences from DynamoDB
    (Corresponds to Extra Credit: Retrieve State)
    """
    if not user_id:
        return None
    try:
        response = state_table.get_item(Key={'userId': user_id})
        return response.get('Item')
    except Exception as e:
        logger.error(f"❌ Error reading state from DynamoDB: {e}")
        return None


def dispatch(intent_request):
    """Route dispatcher"""
    intent_name = intent_request['sessionState']['intent']['name']

    # Get Session ID (user identifier from frontend)
    # This is crucial for distinguishing different users
    user_id = intent_request.get('sessionId', 'unknown_user')
    slots = get_slots(intent_request)

    # Scenario A: GreetingIntent (greeting - triggers Extra Credit logic)
    if intent_name == 'GreetingIntent':
        # Check if this user has visited before
        last_search = get_user_state(user_id)

        if last_search:
            # Returning user logic
            logger.info(f"Returning user detected: {user_id}")

            # Automatically place order: reuse LF2 (Separate Lambda)
            # Construct the same message as before and send to SQS
            msg_body = {
                'Location': last_search['Location'],
                'Cuisine': last_search['Cuisine'],
                'DiningTime': last_search['DiningTime'],
                'NumberOfPeople': last_search['NumberOfPeople'],
                'Email': last_search['Email']
            }

            # Send to SQS -> triggers LF2 -> sends email
            sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(msg_body))

            # Reply to user
            msg = (
                f"Welcome back! Based on your previous search for {last_search['Cuisine']} in {last_search['Location']}, "
                f"I've automatically sent fresh recommendations to {last_search['Email']}! "
                "Do you want to start a new search?")

            return close({}, 'GreetingIntent', 'Fulfilled', msg)

        else:
            # New user logic
            return close({}, 'GreetingIntent', 'Fulfilled', "Hi there, Nice to meet you. How can I help?")

    # Scenario B: DiningSuggestionsIntent (ordering - includes validation and saving)
    if intent_name == 'DiningSuggestionsIntent':
        source = intent_request['invocationSource']

        # Step 1: Validation
        if source == 'DialogCodeHook':
            validation_result = validate_input(slots)

            if not validation_result['isValid']:
                # If validation fails (e.g., user said "New Delhi"), clear the slot and ask user to re-enter
                slots[validation_result['violatedSlot']] = None
                return elicity_slot(
                    intent_request['sessionState'].get('sessionAttributes', {}),
                    intent_name,
                    slots,
                    validation_result['violatedSlot'],
                    validation_result['message']
                )

            # Validation passed, continue to next slot
            return delegate(
                intent_request['sessionState'].get('sessionAttributes', {}),
                slots,
                intent_name
            )

        # Step 2: Fulfillment
        elif source == 'FulfillmentCodeHook':
            # Collect data
            data = {
                'Location': slots['Location']['value']['originalValue'],
                'Cuisine': slots['Cuisine']['value']['originalValue'],
                'DiningTime': slots['DiningTime']['value']['originalValue'],
                'NumberOfPeople': slots['NumberOfPeople']['value']['originalValue'],
                'Email': slots['Email']['value']['originalValue']
            }

            # Send to SQS (core business logic)
            sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(data))

            # Save state (Extra Credit - prepare for next time)
            save_user_state(user_id, data)

            return close(
                intent_request['sessionState'].get('sessionAttributes', {}),
                intent_name,
                'Fulfilled',
                "You're all set. Expect my suggestions shortly! Have a good day."
            )

    # Handle ThankYouIntent and other intents
    return close({}, 'GreetingIntent', 'Fulfilled', "You're welcome!")


def lambda_handler(event, context):
    logger.debug(f"Event: {json.dumps(event)}")
    return dispatch(event)