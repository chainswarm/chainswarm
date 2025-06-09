import pytest
from neo4j import GraphDatabase
from packages.api.tools.similarity_search import SimilaritySearchTool
from packages.api.services.similarity_search_service import SimilaritySearchService
from packages.indexers.base import get_memgraph_connection_string


@pytest.fixture
def graph_connection():
    """
    Fixture to create a connection to the Memgraph database for the 'torus' network.
    """
    # Connect to the real Memgraph database for 'torus' network
    graph_db_url, graph_db_user, graph_db_password = get_memgraph_connection_string('torus')
    driver = GraphDatabase.driver(
        graph_db_url,
        auth=(graph_db_user, graph_db_password)
    )
    yield driver
    driver.close()


@pytest.fixture
def similarity_tool(graph_connection):
    """
    Fixture to create a SimilaritySearchTool instance.
    """
    return SimilaritySearchTool(graph_connection)


@pytest.fixture
def similarity_service(graph_connection):
    """
    Fixture to create a SimilaritySearchService instance.
    """
    return SimilaritySearchService(graph_connection)


@pytest.fixture
def sample_address(graph_connection):
    """
    Fixture to get a sample address with embeddings from the database.
    """
    with graph_connection.session() as session:
        result = session.run("""
        MATCH (a:Address) 
        WHERE a.joint_embedding IS NOT NULL 
        RETURN a.address AS address
        LIMIT 1
        """)
        record = result.single()
        if not record:
            pytest.skip("No suitable address found with embeddings")
        return record["address"]


def test_get_similarity_search_schema(similarity_tool):
    """
    Test that the get_similarity_search_schema method returns a valid schema.
    """
    schema = similarity_tool.get_similarity_search_schema()
    
    # Verify schema structure
    assert "embedding_types" in schema
    assert "query_types" in schema
    assert "similarity_metrics" in schema
    assert "pattern_structures" in schema
    assert "example_queries" in schema
    
    # Verify embedding types
    assert "financial" in schema["embedding_types"]
    assert "temporal" in schema["embedding_types"]
    assert "network" in schema["embedding_types"]
    assert "joint" in schema["embedding_types"]
    
    # Verify dimensions
    assert schema["embedding_types"]["financial"]["dimensions"] == 6
    assert schema["embedding_types"]["temporal"]["dimensions"] == 4
    assert schema["embedding_types"]["network"]["dimensions"] == 4
    assert schema["embedding_types"]["joint"]["dimensions"] == 14


def test_address_similarity_search(similarity_tool, sample_address):
    """
    Test finding addresses similar to a reference address using the SimilaritySearchTool.
    """
    # Test with joint embedding type
    query = {
        "query_type": "by_address",
        "reference_address": sample_address,
        "embedding_type": "joint",
        "limit": 5,
        "similarity_metric": "cosine"
    }
    
    # Execute the query
    results = similarity_tool.similarity_search_query(query)
    
    # Print results for debugging
    print(f"\nAddress similarity search results for {sample_address}:")
    if results.get('error'):
        print(f"Error: {results['error']}")
    else:
        print(f"Found {results.get('count', 0)} similar addresses")
        for i, result in enumerate(results.get('results', [])[:2]):
            print(f"  {i+1}. Address: {result.get('address')}")
            print(f"     Similarity Score: {result.get('similarity_score')}")
    
    # Verify results
    assert 'error' not in results, f"Query returned error: {results.get('error')}"
    assert results.get('count', 0) > 0, "Should find at least one similar address"
    assert len(results.get('results', [])) > 0, "Results list should not be empty"
    
    # First result should be the address itself with high similarity
    assert results['results'][0]['address'] == sample_address
    assert results['results'][0]['similarity_score'] > 0.99, "First result should be the query address with high similarity score"


def test_compare_tool_and_service(similarity_tool, similarity_service, sample_address):
    """
    Compare results from SimilaritySearchTool and SimilaritySearchService.
    """
    # Tool query
    tool_query = {
        "query_type": "by_address",
        "reference_address": sample_address,
        "embedding_type": "joint",
        "limit": 3,
        "similarity_metric": "cosine"
    }
    
    # Execute the tool query
    tool_results = similarity_tool.similarity_search_query(tool_query)
    
    # Execute the service query
    service_results = similarity_service.find_similar_addresses_by_address(
        address=sample_address,
        embedding_type="joint",
        limit=3,
        similarity_metric="cosine"
    )
    
    # Print comparison for debugging
    print("\nComparing SimilaritySearchTool vs SimilaritySearchService:")
    print(f"Tool found {tool_results.get('count', 0)} results")
    print(f"Service found {len(service_results) if service_results else 0} results")
    
    if tool_results.get('results') and service_results:
        # Compare first result addresses
        tool_first_address = tool_results['results'][0]['address']
        service_first_address = service_results[0]['address']
        print(f"Tool first result address: {tool_first_address}")
        print(f"Service first result address: {service_first_address}")
        
        # Compare similarity scores
        tool_first_score = tool_results['results'][0]['similarity_score']
        service_first_score = service_results[0]['similarity_score']
        print(f"Tool first result similarity score: {tool_first_score}")
        print(f"Service first result similarity score: {service_first_score}")
    
    # Verify results
    assert tool_results.get('count', 0) > 0, "Tool should find at least one similar address"
    assert service_results and len(service_results) > 0, "Service should find at least one similar address"
    
    # Both should return the same first result (the query address)
    assert tool_results['results'][0]['address'] == sample_address
    assert service_results[0]['address'] == sample_address


def test_financial_pattern_similarity(similarity_tool):
    """
    Test finding addresses similar to a financial pattern.
    """
    # Create a financial pattern
    financial_pattern = {
        'volume_in': 0.8 * 1e8,
        'volume_out': 0.2 * 1e8,
        'volume_differential': 0.1 * 1e8,
        'log_transfer_count': 1.2,
        'outgoing_tx_avg_ratio': 0.5,
        'incoming_tx_avg_ratio': 0.3
    }
    
    # Create the query
    query = {
        "query_type": "by_financial_pattern",
        "embedding_type": "financial",
        "financial_pattern": financial_pattern,
        "limit": 5,
        "similarity_metric": "cosine"
    }
    
    # Execute the query
    results = similarity_tool.similarity_search_query(query)
    
    # Print results for debugging
    print(f"\nFinancial pattern similarity search results:")
    if results.get('error'):
        print(f"Error: {results['error']}")
    else:
        print(f"Found {results.get('count', 0)} similar addresses")
        for i, result in enumerate(results.get('results', [])[:2]):
            print(f"  {i+1}. Address: {result.get('address')}")
            print(f"     Similarity Score: {result.get('similarity_score')}")
    
    # Verify results
    assert 'error' not in results, f"Query returned error: {results.get('error')}"
    assert results.get('count', 0) > 0, "Should find at least one similar address"
    assert len(results.get('results', [])) > 0, "Results list should not be empty"