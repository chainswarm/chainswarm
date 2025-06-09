import pytest
from neo4j import GraphDatabase
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
def similarity_service(graph_connection):
    """
    Fixture to create a SimilaritySearchService instance.
    """
    return SimilaritySearchService(graph_connection)


@pytest.fixture
def sample_addresses(graph_connection):
    """
    Fixture to get sample addresses with embeddings from the database.
    """
    with graph_connection.session() as session:
        result = session.run("""
        MATCH (a:Address) 
        WHERE a.financial_embedding IS NOT NULL 
          AND a.temporal_embedding IS NOT NULL 
          AND a.network_embedding IS NOT NULL 
          AND a.joint_embedding IS NOT NULL 
        RETURN a.address AS address, 
               a.financial_embedding AS financial_embedding,
               a.temporal_embedding AS temporal_embedding,
               a.network_embedding AS network_embedding,
               a.joint_embedding AS joint_embedding
        LIMIT 5
        """)
        addresses = [record for record in result]
        if not addresses:
            pytest.skip("No suitable addresses found with embeddings")
        return addresses


def test_embedding_dimensions(similarity_service):
    """
    Test that the embedding dimensions match those in money_flow_indexer.py.
    """
    assert similarity_service._get_embedding_dimension('financial') == 6
    assert similarity_service._get_embedding_dimension('temporal') == 4
    assert similarity_service._get_embedding_dimension('network') == 4
    assert similarity_service._get_embedding_dimension('joint') == 14


def test_vector_dimensions_from_address(similarity_service, sample_addresses):
    """
    Test that the vectors retrieved from addresses have the correct dimensions.
    """
    if not sample_addresses:
        pytest.skip("No sample addresses available")
    
    address = sample_addresses[0]["address"]
    
    # Test financial embedding
    financial_vector = similarity_service._get_vector_from_address(address, "financial")
    assert len(financial_vector) == 6
    
    # Test temporal embedding
    temporal_vector = similarity_service._get_vector_from_address(address, "temporal")
    assert len(temporal_vector) == 4
    
    # Test network embedding
    network_vector = similarity_service._get_vector_from_address(address, "network")
    assert len(network_vector) == 4
    
    # Test joint embedding
    joint_vector = similarity_service._get_vector_from_address(address, "joint")
    assert len(joint_vector) == 14


def test_direct_financial_vector_search(graph_connection):
    """
    Test direct Memgraph vector search for financial embeddings.
    """
    with graph_connection.session() as session:
        # Test vector search using the example from money_flow_indexer.py
        result = session.run("""
        CALL vector_search.search("FinancialEmbeddings", 5, [0.8, 0.2, 0.1, 1.2, 0.5, 0.3])
        YIELD node, similarity
        RETURN node.address AS address, similarity
        """)
        results = [record for record in result]
        assert len(results) > 0, "Financial vector search should return results"


def test_direct_temporal_vector_search(graph_connection):
    """
    Test direct Memgraph vector search for temporal embeddings.
    """
    with graph_connection.session() as session:
        # Test vector search using the example from money_flow_indexer.py
        result = session.run("""
        CALL vector_search.search("TemporalEmbeddings", 5, [0.01, 0.005, 0.8, 0.6])
        YIELD node, similarity
        RETURN node.address AS address, similarity
        """)
        results = [record for record in result]
        assert len(results) > 0, "Temporal vector search should return results"


def test_direct_network_vector_search(graph_connection):
    """
    Test direct Memgraph vector search for network embeddings.
    """
    with graph_connection.session() as session:
        # Test vector search using the example from money_flow_indexer.py
        result = session.run("""
        CALL vector_search.search("NetworkEmbeddings", 5, [0.5, 42, 10, 15])
        YIELD node, similarity
        RETURN node.address AS address, similarity
        """)
        results = [record for record in result]
        assert len(results) > 0, "Network vector search should return results"


def test_direct_joint_vector_search(graph_connection):
    """
    Test direct Memgraph vector search for joint embeddings.
    """
    with graph_connection.session() as session:
        # Test vector search using the example from money_flow_indexer.py
        result = session.run("""
        CALL vector_search.search("JointEmbeddings", 5, [0.8,0.2,0.1,1.2,0.5,0.3,0.01,0.005,0.8,0.6,0.5,42,10,15])
        YIELD node, similarity
        RETURN node.address AS address, similarity
        """)
        results = [record for record in result]
        assert len(results) > 0, "Joint vector search should return results"


def test_similarity_service_financial_pattern(similarity_service):
    """
    Test SimilaritySearchService with a financial pattern.
    """
    # Create a financial pattern based on the example vector
    financial_pattern = {
        'volume_in': 0.8 * 1e8,
        'volume_out': 0.2 * 1e8,
        'volume_differential': 0.1 * 1e8,
        'log_transfer_count': 1.2,
        'outgoing_tx_avg_ratio': 0.5,
        'incoming_tx_avg_ratio': 0.3
    }
    
    # Test the service
    results = similarity_service.find_similar_addresses(
        embedding_type="financial",
        query_type="by_financial_pattern",
        financial_pattern=financial_pattern,
        limit=5
    )
    # Print results for debugging
    print(f"\nFinancial pattern search results:")
    if results:
        print(f"Found {len(results)} results")
        print(f"First result: {results[0]}")
    else:
        print("No results found")
        
    assert results is not None
    assert len(results) > 0, "SimilaritySearchService should return results for financial pattern"


def test_similarity_service_temporal_pattern(similarity_service):
    """
    Test SimilaritySearchService with a temporal pattern.
    """
    # Create a temporal pattern based on the example vector
    temporal_pattern = {
        'last_transfer_timestamp': 0.01 * 1e12,
        'first_transfer_timestamp': 0.005 * 1e12,
        'avg_outgoing_tx_frequency': 0.8,
        'avg_incoming_tx_frequency': 0.6
    }
    
    # Test the service
    results = similarity_service.find_similar_addresses(
        embedding_type="temporal",
        query_type="by_temporal_pattern",
        temporal_pattern=temporal_pattern,
        limit=5
    )
    # Print results for debugging
    print(f"\nTemporal pattern search results:")
    if results:
        print(f"Found {len(results)} results")
        print(f"First result: {results[0]}")
    else:
        print("No results found")
        
    assert results is not None
    assert len(results) > 0, "SimilaritySearchService should return results for temporal pattern"


def test_similarity_service_network_pattern(similarity_service):
    """
    Test SimilaritySearchService with a network pattern.
    """
    # Create a network pattern based on the example vector
    network_pattern = {
        'page_rank': 0.5,
        'community_id': 42,
        'unique_senders': 10,
        'unique_receivers': 15
    }
    
    # Test the service
    results = similarity_service.find_similar_addresses(
        embedding_type="network",
        query_type="by_network_pattern",
        network_pattern=network_pattern,
        limit=5
    )
    # Print results for debugging
    print(f"\nNetwork pattern search results:")
    if results:
        print(f"Found {len(results)} results")
        print(f"First result: {results[0]}")
    else:
        print("No results found")
        
    assert results is not None
    assert len(results) > 0, "SimilaritySearchService should return results for network pattern"


def test_similarity_service_combined_pattern(similarity_service):
    """
    Test SimilaritySearchService with a combined pattern.
    """
    # Create a combined pattern based on the example vector
    combined_pattern = {
        # Financial components
        'volume_in': 0.8 * 1e8,
        'volume_out': 0.2 * 1e8,
        'volume_differential': 0.1 * 1e8,
        'log_transfer_count': 1.2,
        'outgoing_tx_avg_ratio': 0.5,
        'incoming_tx_avg_ratio': 0.3,
        
        # Temporal components
        'last_transfer_timestamp': 0.01 * 1e12,
        'first_transfer_timestamp': 0.005 * 1e12,
        'avg_outgoing_tx_frequency': 0.8,
        'avg_incoming_tx_frequency': 0.6,
        
        # Network components
        'page_rank': 0.5,
        'community_id': 42,
        'unique_senders': 10,
        'unique_receivers': 15
    }
    
    # Test the service
    results = similarity_service.find_similar_addresses(
        embedding_type="joint",
        query_type="by_combined_pattern",
        combined_pattern=combined_pattern,
        limit=5
    )
    # Print results for debugging
    print(f"\nCombined pattern search results:")
    if results:
        print(f"Found {len(results)} results")
        print(f"First result: {results[0]}")
    else:
        print("No results found")
        
    assert results is not None
    assert len(results) > 0, "SimilaritySearchService should return results for combined pattern"


def test_similarity_service_by_address(similarity_service, graph_connection):
    """
    Test SimilaritySearchService with an existing address.
    """
    # Find an existing address with embeddings
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
        address = record["address"]
    
    # Test the service
    results = similarity_service.find_similar_addresses_by_address(
        address=address,
        embedding_type="joint",
        limit=5
    )
    # Print results for debugging
    print(f"\nSearch for address: {address}")
    if results:
        print(f"Found {len(results)} results")
        print(f"First result: {results[0]}")
        if len(results) > 1:
            print(f"Second result: {results[1]}")
    else:
        print("No results found")
        
    assert results is not None
    assert len(results) > 0, "SimilaritySearchService should return results for address search"
    # First result should be the address itself with high similarity
    assert results[0]["address"] == address
    assert results[0]["similarity_score"] > 0.99, "First result should be the query address with high similarity score"