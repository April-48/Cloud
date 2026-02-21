import json
import boto3
import random
import urllib.request
import base64
from boto3.dynamodb.conditions import Key

# Configuration Section (Please modify here!)
# 1. OpenSearch domain endpoint (without https://)
# Copy from your previous verify_search.py
OS_HOST = 'search-restaurants-x4ujt233i4kn34uncyxcwn4lzm.us-east-1.es.amazonaws.com'
OS_INDEX = 'restaurants'
OS_AUTH = ('admin', 'Hyl8588469!!')  # (username, password)

# 2. SQS queue URL
# Go to SQS console -> click Q1 -> copy URL
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/851311377139/DiningQueue'

# 3. Sender email address
# Must be an email address verified (Verified) in SES
SENDER_EMAIL = ' yuelihe2@gmail.com'

# Other configuration (usually no need to change)
REGION = 'us-east-1'
TABLE_NAME = 'yelp-restaurants'

# Initialize AWS clients
sqs = boto3.client('sqs', region_name=REGION)
ses = boto3.client('ses', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(TABLE_NAME)


def get_sqs_messages():
    """Poll SQS for messages"""
    try:
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=5,  # Maximum 5 messages per poll
            WaitTimeSeconds=5,  # Long polling
            VisibilityTimeout=30,  # Processing timeout
            MessageAttributeNames=['All']
        )
        return response.get('Messages', [])
    except Exception as e:
        print(f"❌ SQS Error: {e}")
        return []


def delete_sqs_message(receipt_handle):
    """Delete message after successful processing"""
    sqs.delete_message(
        QueueUrl=SQS_QUEUE_URL,
        ReceiptHandle=receipt_handle
    )


def query_opensearch(cuisine):
    """Query OpenSearch to get restaurant IDs"""
    print(f"Searching OpenSearch for: {cuisine}")
    url = f'https://{OS_HOST}/{OS_INDEX}/_search?q=Cuisine:{cuisine}&size=50'

    # Construct authentication header
    auth_str = f'{OS_AUTH[0]}:{OS_AUTH[1]}'
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    headers = {
        'Authorization': f'Basic {b64_auth}',
        'Content-Type': 'application/json'
    }

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode())
            hits = result['hits']['hits']
            # Extract IDs
            ids = [hit['_source']['RestaurantID'] for hit in hits]
            print(f"Found {len(ids)} restaurants in OpenSearch.")
            return ids
    except Exception as e:
        print(f"❌ OpenSearch Error: {e}")
        return []


def get_restaurant_details(ids):
    """Query DynamoDB for restaurant details by ID"""
    results = []
    # Randomly select 3 restaurants
    selected_ids = random.sample(ids, min(3, len(ids)))

    for r_id in selected_ids:
        try:
            resp = table.get_item(Key={'BusinessID': r_id})
            if 'Item' in resp:
                results.append(resp['Item'])
        except Exception as e:
            print(f"DynamoDB Error for ID {r_id}: {e}")
    return results


def send_email(recipient, cuisine, restaurants):
    """Send email with restaurant recommendations"""
    text_body = f"Hello! Here are my {cuisine} restaurant suggestions for you:\n\n"

    for i, r in enumerate(restaurants, 1):
        name = r.get('Name', 'Unknown')
        address = r.get('Address', 'Unknown Address')
        rating = r.get('Rating', 'N/A')

        # Address in DynamoDB might be a list
        if isinstance(address, list):
            address = ", ".join(address)

        text_body += f"{i}. {name}\n   Address: {address}\n   Rating: {rating}/5\n\n"

    text_body += "Enjoy your meal!\n- Your Dining Concierge Bot"

    try:
        ses.send_email(
            Source=SENDER_EMAIL,
            Destination={'ToAddresses': [recipient]},
            Message={
                'Subject': {'Data': f"Your {cuisine} Suggestions"},
                'Body': {'Text': {'Data': text_body}}
            }
        )
        print(f"✅ Email sent successfully to {recipient}")
        return True
    except Exception as e:
        print(f"❌ SES Error: {e}")
        return False


def lambda_handler(event, context):
    """Entry point function"""
    print("LF2 Waking up... Polling SQS...")

    # Pull messages from SQS (7.B.i)
    messages = get_sqs_messages()

    if not messages:
        print("No messages found. Going back to sleep.")
        return {'statusCode': 200, 'body': 'No messages'}

    for msg in messages:
        receipt_handle = msg['ReceiptHandle']
        try:
            body = json.loads(msg['Body'])
            print(f"Processing Request: {body}")

            cuisine = body.get('Cuisine')
            email = body.get('Email')

            if not cuisine or not email:
                print("Invalid message format, deleting.")
                delete_sqs_message(receipt_handle)
                continue

            # Query OpenSearch (7.B.ii)
            ids = query_opensearch(cuisine)

            if not ids:
                print(f"No restaurants found for {cuisine}")
                delete_sqs_message(receipt_handle)  # Delete even if not found to prevent infinite loop
                continue

            # Query DynamoDB (7.C)
            details = get_restaurant_details(ids)

            # Send email (7.B.iv)
            if send_email(email, cuisine, details):
                # Only delete message if email was sent successfully
                delete_sqs_message(receipt_handle)

        except Exception as e:
            print(f"Error processing message: {e}")

    return {'statusCode': 200, 'body': 'Processed'}
