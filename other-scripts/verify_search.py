# File name: verify_search.py
from opensearchpy import OpenSearch, RequestsHttpConnection
import json

# Please modify here
HOST = 'search-restaurants-x4ujt233i4kn34uncyxcwn4lzm.us-east-1.es.amazonaws.com' 
AUTH = ('admin', 'Hyl8588469!!')

def test_search():
    client = OpenSearch(
        hosts=[{'host': HOST, 'port': 443}],
        http_auth=AUTH,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # Search for "Chinese" cuisine
    query = {
        "query": {
            "match": {
                "Cuisine": "Chinese"
            }
        }
    }

    print("Searching for 'Chinese' restaurants...")
    response = client.search(
        body=query,
        index="restaurants"
    )

    print("\n--- Search Results ---")
    print(f"Total results found: {response['hits']['total']['value']}")
    
    # Print first 3 results
    for hit in response['hits']['hits'][:3]:
        print(f"ID: {hit['_source']['RestaurantID']}, Cuisine: {hit['_source']['Cuisine']}")

if __name__ == '__main__':
    test_search()
