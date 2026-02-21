import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import bulk

# Configuration Section
# OpenSearch domain endpoint (without https://)
# Example: search-restaurants-xxxxxx.us-east-1.es.amazonaws.com
HOST = 'search-restaurants-x4ujt233i4kn34uncyxcwn4lzm.us-east-1.es.amazonaws.com' 

# Master username and password you set earlier
AUTH = ('admin', 'Hyl8588469!!') 

REGION = 'us-east-1'
DYNAMODB_TABLE = 'yelp-restaurants'
INDEX_NAME = 'restaurants'

def get_all_restaurants():
    """Scan DynamoDB to get all restaurant data"""
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    table = dynamodb.Table(DYNAMODB_TABLE)
    
    response = table.scan()
    data = response['Items']
    
    # If data is large, DynamoDB will paginate, need to loop to get all
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
        
    print(f"Total restaurants found in DynamoDB: {len(data)}")
    return data

def push_to_opensearch(restaurants):
    """Bulk push data to OpenSearch"""
    
    # Connect to OpenSearch
    client = OpenSearch(
        hosts=[{'host': HOST, 'port': 443}],
        http_auth=AUTH,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    
    print("Connecting to OpenSearch...")
    print(client.info()) # Test connection

    # Prepare bulk data
    actions = []
    for r in restaurants:
        action = {
            "_index": INDEX_NAME,
            "_id": r['BusinessID'], # Use BusinessID as document ID
            "_source": {
                "RestaurantID": r['BusinessID'],
                "Cuisine": r['Cuisine']
            }
        }
        actions.append(action)
    
    # Bulk upload
    if actions:
        success, failed = bulk(client, actions)
        print(f"Successfully inserted: {success}")
        print(f"Failed: {failed}")
    else:
        print("No data to insert.")

if __name__ == '__main__':
    print("Reading from DynamoDB...")
    data = get_all_restaurants()
    
    print("Pushing to OpenSearch...")
    # Note: If OpenSearch is still being created, this will error. Wait until status is Active before running.
    try:
        push_to_opensearch(data)
    except Exception as e:
        print(f"Error: {e}")
        print("Tip: Please check if OpenSearch URL is correct, or if the domain is in Active status.")