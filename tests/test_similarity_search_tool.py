import os
import sys
import json
from neo4j import GraphDatabase

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from packages.api.tools.similarity_search import SimilaritySearchTool

def test_get_schema():
    """Test the get_similarity_search_schema method"""
    # Create a mock driver (we won't actually connect to the database for schema test)
    mock_driver = GraphDatabase.driver("bolt://localhost:7687")
    
    # Create the tool
    tool = SimilaritySearchTool(mock_driver)
    
    # Get the schema
    schema = tool.get_similarity_search_schema()
    
    # Print the schema in a readable format
    print("Similarity Search Schema:")
    print(json.dumps(schema, indent=2))
    
    # Close the tool
    tool.close()
    
    return schema

def test_similarity_search_query(uri, username, password, network="torus"):
    """Test the similarity_search_query method with a real database connection"""
    # Create a real driver
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    # Create the tool
    tool = SimilaritySearchTool(driver)
    
    # Example query - find addresses similar to a reference address
    query = {
        "query_type": "by_address",
        "reference_address": "5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f",  # Example address
        "embedding_type": "joint",
        "limit": 5,
        "similarity_metric": "cosine"
    }
    
    # Execute the query
    print(f"Executing similarity search query: {json.dumps(query, indent=2)}")
    results = tool.similarity_search_query(query)
    
    # Print the results
    print(f"Query results ({results.get('count', 0)} matches):")
    if results.get('error'):
        print(f"Error: {results['error']}")
    else:
        for i, result in enumerate(results.get('results', [])):
            print(f"Result {i+1}:")
            print(f"  Address: {result.get('address')}")
            print(f"  Similarity Score: {result.get('similarity_score')}")
            print(f"  Volume In: {result.get('volume_in')}")
            print(f"  Volume Out: {result.get('volume_out')}")
            print(f"  Transfer Count: {result.get('transfer_count')}")
            print()
    
    # Close the tool
    tool.close()
    
    return results

if __name__ == "__main__":
    # Test the schema
    test_get_schema()
    
    # If you want to test the query functionality, uncomment and provide connection details
    # uri = "bolt://localhost:7687"
    # username = "memgraph"
    # password = ""
    # test_similarity_search_query(uri, username, password)