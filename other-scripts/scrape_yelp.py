import requests
import boto3
import json
import time
from decimal import Decimal
from datetime import datetime

# Configuration Section
# Your Yelp API Key (please replace)
API_KEY = 'YvkreEIehZsyTUTbwMxNgUMpbDTkBELuboEt9_kDHjITQf6Vv6DFmEOKrPJjzk2-D_P07jkpgQVsjBaNAStNPYoryGc3berbaykTeuMxqC4sZM1myOR_CGkXu9yNaXYx'

# Define cuisines to scrape (you can modify, but must be Yelp-supported categories)
CUISINES = [
    'chinese', 
    'indian', 
    'italian', 
    'mexican', 
    'japanese', 
    'thai',           # Thai cuisine (many in Manhattan)
    'french',         # French cuisine
    'burgers',        # Burgers (American fast food)
    'pizza',          # Pizza (NYC specialty!)
    'korean',         # Korean cuisine (K-town)
    'mediterranean',  # Mediterranean cuisine
    'vietnamese'      # Vietnamese cuisine
]
# Target location, assignment requires Manhattan, can add more later
LOCATION = 'Manhattan'

# Number of restaurants to scrape per cuisine (Yelp returns max 50 per request, so we need to loop 4 times)
TARGET_PER_CUISINE = 200
SEARCH_LIMIT = 50 

# AWS configuration
DYNAMODB_TABLE = 'yelp-restaurants'
REGION = 'us-east-1'

def get_yelp_data(cuisine, offset):
    """Fetch data from Yelp API"""
    url = "https://api.yelp.com/v3/businesses/search"
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    params = {
        "term": f"{cuisine} restaurants",
        "location": LOCATION,
        "limit": SEARCH_LIMIT,
        "offset": offset, # Used for pagination
        "sort_by": "best_match"
    }
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json().get('businesses', [])
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        return []

def save_to_dynamodb(restaurants, cuisine_type):
    """Save data to DynamoDB"""
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    table = dynamodb.Table(DYNAMODB_TABLE)
    
    with table.batch_writer() as batch:
        for r in restaurants:
            try:
                # Check if required fields exist, skip if not
                if not r.get('id') or not r.get('name'):
                    continue

                # Construct item that meets DynamoDB requirements
                # Note: DynamoDB doesn't support float, must convert to Decimal
                item = {
                    'BusinessID': r['id'], # Used as primary key
                    'Name': r['name'],
                    'Address': r['location']['display_address'], # This is a list
                    'Coordinates': {
                        'latitude': Decimal(str(r['coordinates']['latitude'])) if r['coordinates']['latitude'] else None,
                        'longitude': Decimal(str(r['coordinates']['longitude'])) if r['coordinates']['longitude'] else None
                    },
                    'NumberOfReviews': r['review_count'],
                    'Rating': Decimal(str(r['rating'])),
                    'ZipCode': r['location'].get('zip_code', 'N/A'),
                    'Cuisine': cuisine_type, # Store cuisine field for later use
                    'insertedAtTimestamp': datetime.now().isoformat()
                }

                # Write to database
                batch.put_item(Item=item)
                print(f"Saved: {r['name']}")
                
            except Exception as e:
                print(f"Failed to save {r.get('name', 'Unknown')}: {e}")

def main():
    print("Starting scraping...")
    
    for cuisine in CUISINES:
        print(f"\n--- Scraping Cuisine: {cuisine} ---")
        count = 0
        
        # Loop until reaching 200 restaurants
        while count < TARGET_PER_CUISINE:
            print(f"Fetching {cuisine} offset {count}...")
            batch_data = get_yelp_data(cuisine, count)
            
            if not batch_data:
                print("No more data found.")
                break
                
            save_to_dynamodb(batch_data, cuisine)
            count += len(batch_data)
            
            # Polite pause to avoid triggering Yelp rate limits
            time.sleep(1)

    print("\n✅ All done! Data imported to DynamoDB.")

if __name__ == '__main__':
    main()
