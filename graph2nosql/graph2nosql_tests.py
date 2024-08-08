from graph2nosql import NoSQLKnowledgeGraph
from firestore_kg import FirestoreKG
from data_model import NodeData, EdgeData, CommunityData

import unittest
from abc import ABC, abstractmethod

import networkx as nx


class _NoSQLKnowledgeGraphTests(ABC):
    """
    Abstract base class to define test cases for NoSQLKnowledgeGraph implementations.

    Concrete test classes for specific NoSQL databases should inherit from this class
    and implement the required abstract methods.
    """

    @abstractmethod
    def create_kg_instance(self) -> NoSQLKnowledgeGraph:
        """Create and return an instance of the NoSQLKnowledgeGraph implementation."""
        pass

    def setUp(self):
        """Set up for test methods."""
        self.kg = self.create_kg_instance()
        # Add any setup logic specific to your NoSQL database here

    def test_add_and_remove_node(self):
        # Add a node
        node_data = NodeData(
            node_uid="added_test_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        self.kg.add_node(node_uid="added_test_node_1", node_data=node_data)

        # Retrieve the node and verify its data
        retrieved_node_data = self.kg.get_node(node_uid="added_test_node_1")
        print(retrieved_node_data)
        self.assertEqual(retrieved_node_data, node_data) # type: ignore

        # Remove the node
        self.kg.remove_node(node_uid="added_test_node_1")

        # Try to retrieve the node again (should raise KeyError)
        with self.assertRaises(KeyError): # type: ignore
            self.kg.get_node(node_uid="added_test_node_1")

    def test_update_node(self):
        # Add a node
        node_data = NodeData(
            node_uid="test_update_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        self.kg.add_node(node_uid="test_update_node_1", node_data=node_data)

        # Retrieve the node and verify its data
        retrieved_node_data = self.kg.get_node(node_uid="test_update_node_1")
        self.assertEqual(retrieved_node_data, node_data) # type: ignore
        
        # Update the node
        updated_node_data = NodeData(
            node_uid="test_update_node_1",
            node_title="Updated Test Node 1", # updated title
            node_type="Person",
            node_description="This is an updated test node", # updated description
            node_degree=1,  # Updated degree
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        self.kg.update_node(node_uid="test_update_node_1", node_data=updated_node_data)

        # Retrieve the node again and verify the update
        retrieved_updated_node_data = self.kg.get_node(node_uid="test_update_node_1")
        self.assertEqual(retrieved_updated_node_data, updated_node_data) # type: ignore

        # Remove the node
        self.kg.remove_node(node_uid="test_update_node_1")

    def add_node_with_egde(self):
        # Add a node with edge to other node that doesn't exist
        node_data = NodeData(
            node_uid="test_egde_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=["fake node",],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        
        # Assert that adding the node raises a KeyError (or a more specific exception you handle)
        with self.assertRaises(KeyError):  # type: ignore # Adjust exception type if needed
            self.kg.add_node(node_uid="test_egde_node_1", node_data=node_data)

        # Add valid nodes (required for edges)
        node_data_1 = NodeData(
            node_uid="test_egde_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        node_data_2 = NodeData(
            node_uid="test_egde_node_2",
            node_title="Test Node 2",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_2",
            edges_to=[],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )

        node_data_3 = NodeData(
            node_uid="test_egde_node_3",
            node_title="Test Node 2",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_2",
            edges_to=["test_node_1",],
            edges_from=["test_node_2",],
            embedding=[0.4, 0.5, 0.6],
        )

        self.kg.add_node(node_uid="test_egde_node_1", node_data=node_data_1)
        self.kg.add_node(node_uid="test_egde_node_2", node_data=node_data_2)
        self.kg.add_node(node_uid="test_egde_node_3", node_data=node_data_3)

        # Assert that the edges are reflected in the nodes' edge lists
        node1 = self.kg.get_node("test_egde_node_1")
        node2 = self.kg.get_node("test_egde_node_2")
        node3 = self.kg.get_node("test_egde_node_3")

        self.assertIn("test_egde_node_3", node1.edges_from) # type: ignore
        self.assertIn("test_egde_node_3", node2.edges_to) # type: ignore
        self.assertIn("test_egde_node_1", node3.edges_to) # type: ignore
        self.assertIn("test_egde_node_2", node3.edges_from) # type: ignore

        # Clean up
        self.kg.remove_node(node_uid="test_egde_node_1")
        self.kg.remove_node(node_uid="test_egde_node_2")
        self.kg.remove_node(node_uid="test_egde_node_3")

    def test_add_direcred_edge(self):
        """Test adding an edge between nodes."""

        # Add valid nodes (required for edges)
        node_data_1 = NodeData(
            node_uid="test_directed_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        node_data_2 = NodeData(
            node_uid="test_directed_node_2",
            node_title="Test Node 2",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_2",
            edges_to=[],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )
        
        self.kg.add_node(node_uid="test_directed_node_1", node_data=node_data_1)
        self.kg.add_node(node_uid="test_directed_node_2", node_data=node_data_2)

        # add edges between nodes
        edge_data = EdgeData(
            source_uid="test_directed_node_1",
            target_uid="test_directed_node_2",
            description="This is a test egde description"
        )

        self.kg.add_edge(edge_data=edge_data)

        # Assert that the edge is reflected in the nodes' edge lists
        node1 = self.kg.get_node("test_directed_node_1")
        node2 = self.kg.get_node("test_directed_node_2")
        self.assertIn("test_directed_node_2", node1.edges_to) # type: ignore
        self.assertIn("test_directed_node_1", node2.edges_from) # type: ignore

        # Clean Up nodes
        self.kg.remove_node(node_uid="test_directed_node_1")
        self.kg.remove_node(node_uid="test_directed_node_2")

        # Clean Up egdes
        self.kg.remove_edge(source_uid="test_directed_node_1", target_uid="test_directed_node_2")

    def test_add_undirecred_edge(self):
        """Test adding an edge between nodes."""

        # Add valid nodes (required for edges)
        node_data_1 = NodeData(
            node_uid="test_undirected_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        node_data_2 = NodeData(
            node_uid="test_undirected_node_2",
            node_title="Test Node 2",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_2",
            edges_to=[],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )
        
        self.kg.add_node(node_uid="test_undirected_node_1", node_data=node_data_1)
        self.kg.add_node(node_uid="test_undirected_node_2", node_data=node_data_2)

        # add edges between nodes
        edge_data = EdgeData(
            source_uid="test_undirected_node_1",
            target_uid="test_undirected_node_2",
            description="This is a test egde description"
        )

        self.kg.add_edge(edge_data=edge_data, directed=False)

        # Assert that the edge is reflected in the nodes' edge lists
        node1 = self.kg.get_node("test_undirected_node_1")
        node2 = self.kg.get_node("test_undirected_node_2")
        self.assertIn("test_undirected_node_2", node1.edges_to) # type: ignore
        self.assertIn("test_undirected_node_1", node2.edges_to) # type: ignore
        self.assertIn("test_undirected_node_1", node2.edges_from) # type: ignore
        self.assertIn("test_undirected_node_2", node1.edges_from) # type: ignore

        # Clean Up nodes
        self.kg.remove_node(node_uid="test_undirected_node_1")
        self.kg.remove_node(node_uid="test_undirected_node_2")

        # Clean Up egdes
        self.kg.remove_edge(source_uid="test_undirected_node_1", target_uid="test_undirected_node_2")
        self.kg.remove_edge(source_uid="test_undirected_node_2", target_uid="test_undirected_node_1")

    def test_get_edge(self):
        """Test retrieving an existing edge."""
        # 1. Add nodes (required for edges)
        node_data_1 = NodeData(
            node_uid="test_getedge_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        node_data_2 = NodeData(
            node_uid="test_getedge_node_2",
            node_title="Test Node 2",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_2",
            edges_to=[],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )
        self.kg.add_node(node_uid="test_getedge_node_1", node_data=node_data_1)
        self.kg.add_node(node_uid="test_getedge_node_2", node_data=node_data_2)

        # 2. Add an edge
        edge_data = EdgeData(
            source_uid="test_getedge_node_1",
            target_uid="test_getedge_node_2",
            description="This might be a description of the relationship of these two nodes"
        )
        self.kg.add_edge(edge_data=edge_data)

        # Assuming you have a way to retrieve edge data in your implementation
        retrieved_edge_data = self.kg.get_edge(source_uid="test_getedge_node_1", target_uid="test_getedge_node_2")


        new_edge_uid = self.kg._generate_edge_uid(edge_data.source_uid, edge_data.target_uid)
        target_edge_data = EdgeData(
            source_uid="test_getedge_node_1",
            target_uid="test_getedge_node_2",
            description="This might be a description of the relationship of these two nodes",
            edge_uid=new_edge_uid
        )

        self.assertEqual(retrieved_edge_data, target_edge_data) # type: ignore

        # Clean up nodes
        self.kg.remove_node(node_uid="test_getedge_node_1")
        self.kg.remove_node(node_uid="test_getedge_node_2")

        # Clean up edges
        self.kg.remove_edge(source_uid="test_getedge_node_1", target_uid="test_getedge_node_2")

    def test_update_edge(self):
        """Test updating the data of an existing edge."""
        # Add nodes (required for edges)
        node_data_1 = NodeData(
            node_uid="test_edgeupdate_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        node_data_2 = NodeData(
            node_uid="test_edgeupdate_node_2",
            node_title="Test Node 2",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_2",
            edges_to=[],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )
        self.kg.add_node(node_uid="test_edgeupdate_node_1", node_data=node_data_1)
        self.kg.add_node(node_uid="test_edgeupdate_node_2", node_data=node_data_2)

        # Add an edge between
        edge_data = EdgeData(
            source_uid="test_edgeupdate_node_1",
            target_uid="test_edgeupdate_node_2",
            description="This is a boring egde description"
        )

        self.kg.add_edge(
            edge_data=edge_data
        )

        # Update edge with new data
        updated_edge_data = EdgeData(
            source_uid="test_edgeupdate_node_1",
            target_uid="test_edgeupdate_node_2",
            description="Updated much better description"
        )
        self.kg.update_edge(edge_data=updated_edge_data)

        # Verify that the edge data is updated
        retrieved_updated_edge_data = self.kg.get_edge(
            source_uid="test_edgeupdate_node_1", target_uid="test_edgeupdate_node_2"
        )

        validate_edge_data = EdgeData(
            source_uid="test_edgeupdate_node_1",
            target_uid="test_edgeupdate_node_2",
            description="Updated much better description",
            edge_uid=self.kg._generate_edge_uid(
                edge_data.source_uid, edge_data.target_uid
            )
        )
        
        self.assertEqual( # type: ignore
            retrieved_updated_edge_data, validate_edge_data
        )

        # Cleanup edges
        self.kg.remove_edge(source_uid="test_edgeupdate_node_1", target_uid="test_edgeupdate_node_2")

        # Cleanup nodes
        self.kg.remove_node(node_uid="test_edgeupdate_node_1")
        self.kg.remove_node(node_uid="test_edgeupdate_node_2")

    def test_remove_edge(self):
        """Test removing an edge between nodes."""
        # Add nodes (required for edges)
        node_data_1 = NodeData(
            node_uid="test_removeegde_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        node_data_2 = NodeData(
            node_uid="test_removeegde_node_2",
            node_title="Test Node 2",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_2",
            edges_to=["test_node_1",],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )
        self.kg.add_node(node_uid="test_removeegde_node_1", node_data=node_data_1)
        self.kg.add_node(node_uid="test_removeegde_node_2", node_data=node_data_2)

        self.kg.remove_edge(source_uid="test_removeegde_node_2", target_uid="test_removeegde_node_1")

        # Assert that the edge is no longer in the nodes' edge lists
        node1 = self.kg.get_node("test_removeegde_node_1")
        node2 = self.kg.get_node("test_removeegde_node_2")
        self.assertNotIn("test_removeegde_node_2", node1.edges_to) # type: ignore
        self.assertNotIn("test_removeegde_node_2", node1.edges_from) # type: ignore
        self.assertNotIn("test_removeegde_node_1", node2.edges_from) # type: ignore
        self.assertNotIn("test_removeegde_node_1", node2.edges_to) # type: ignore

        # Clean up nodes
        self.kg.remove_node(node_uid="test_removeegde_node_1")
        self.kg.remove_node(node_uid="test_removeegde_node_2")

    def test_get_networkx(self):
        """Test getting the networkx graph."""
        # 1. Add nodes 
        node_data_1 = NodeData(
            node_uid="test_getnx_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        node_data_2 = NodeData(
            node_uid="test_getnx_node_2",
            node_title="Test Node 2",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_2",
            edges_to=[],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )
        self.kg.add_node(node_uid="test_getnx_node_1", node_data=node_data_1)
        self.kg.add_node(node_uid="test_getnx_node_2", node_data=node_data_2)

        # 2. Add an edge
        edge_data = EdgeData(
            source_uid="test_getnx_node_1",
            target_uid="test_getnx_node_2",
            description="Test Edge Description"
        )
        self.kg.add_edge(edge_data=edge_data)

        # 3. Get the NetworkX graph
        self.kg.build_networkx()

        # 4. Assertions
        # Check if the graph is the correct type
        self.assertIsInstance(self.kg.networkx, nx.Graph) # type: ignore
        # Check if the number of nodes is correct
        self.assertEqual(self.kg.networkx.number_of_nodes(), 2) # type: ignore
        # Check if the number of edges is correct
        self.assertEqual(self.kg.networkx.number_of_edges(), 1) # type: ignore
        # Check if specific nodes exist in the graph
        self.assertTrue(self.kg.networkx.has_node("test_getnx_node_1")) # type: ignore
        self.assertTrue(self.kg.networkx.has_node("test_getnx_node_2")) # type: ignore
        # Check if a specific edge exists in the graph
        self.assertTrue(self.kg.networkx.has_edge("test_getnx_node_1", "test_getnx_node_2")) # type: ignore

        # 5. Clean up (optional, depending on your test setup)
        self.kg.remove_edge(source_uid="test_getnx_node_1", target_uid="test_getnx_node_2")
        self.kg.remove_node(node_uid="test_getnx_node_1")
        self.kg.remove_node(node_uid="test_getnx_node_2")

    def test_get_louvain_communities(self):
        """Test getting Louvain communities."""
        # 1. Add nodes
        node_data_1 = NodeData(
            node_uid="test_louvain_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        node_data_2 = NodeData(
            node_uid="test_louvain_node_2",
            node_title="Test Node 2",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_2",
            edges_to=[],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )
        node_data_3 = NodeData(
            node_uid="test_louvain_node_3",
            node_title="Test Node 3",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_3",
            edges_to=[],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )

        node_data_3 = NodeData(
            node_uid="test_louvain_node_4",
            node_title="Test Node 3",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_3",
            edges_to=[],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )

        self.kg.add_node(node_uid="test_louvain_node_1", node_data=node_data_1)
        self.kg.add_node(node_uid="test_louvain_node_2", node_data=node_data_2)
        self.kg.add_node(node_uid="test_louvain_node_3", node_data=node_data_3)
        self.kg.add_node(node_uid="test_louvain_node_4", node_data=node_data_3)

        # 2. Add edges to create a connected structure for community detection
        edge_data_1 = EdgeData(
            source_uid="test_louvain_node_1",
            target_uid="test_louvain_node_2",
            description="Test Edge Description 1"
        )
        edge_data_2 = EdgeData(
            source_uid="test_louvain_node_2",
            target_uid="test_louvain_node_3",
            description="Test Edge Description 2"
        )
        self.kg.add_edge(edge_data=edge_data_1)
        self.kg.add_edge(edge_data=edge_data_2)

        # 3. Get the Louvain communities
        communities = self.kg.get_louvain_communities()

        # 4. Assertions
        # Ensure communities is a list
        self.assertIsInstance(communities, list) # type: ignore
        # We are expecting exactly two communities since one node has no edges
        self.assertTrue(len(communities) == 2) # type: ignore
        # Check if each community is a set (or your expected data structure)
        for community in communities:
            self.assertIsInstance(community, set) # type: ignore

        # 5. Clean up (optional, depending on your test setup)
        self.kg.remove_edge(source_uid="test_louvain_node_1", target_uid="test_louvain_node_2")
        self.kg.remove_edge(source_uid="test_louvain_node_2", target_uid="test_louvain_node_3")
        self.kg.remove_node(node_uid="test_louvain_node_1")
        self.kg.remove_node(node_uid="test_louvain_node_2")
        self.kg.remove_node(node_uid="test_louvain_node_3")
        self.kg.remove_node(node_uid="test_louvain_node_4")

    def test_visualize_graph(self):
        """Test visualizing the graph. This test is not asserting anything.
        It's only creating a visualization for manual inspection."""

        # 1. Add nodes
        node_data_1 = NodeData(
            node_uid="test_vis_node_1",
            node_title="Test Node 1",
            node_type="Person",
            node_description="This is a test node",
            node_degree=0,
            document_id="doc_1",
            edges_to=[],
            edges_from=[],
            embedding=[0.1, 0.2, 0.3],
        )
        node_data_2 = NodeData(
            node_uid="test_vis_node_2",
            node_title="Test Node 2",
            node_type="Person",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_2",
            edges_to=[],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )
        node_data_3 = NodeData(
            node_uid="test_vis_node_3",
            node_title="Test Node 3",
            node_type="Organization",
            node_description="This is another test node",
            node_degree=0,
            document_id="doc_3",
            edges_to=[],
            edges_from=[],
            embedding=[0.4, 0.5, 0.6],
        )
        self.kg.add_node(node_uid="test_vis_node_1", node_data=node_data_1)
        self.kg.add_node(node_uid="test_vis_node_2", node_data=node_data_2)
        self.kg.add_node(node_uid="test_vis_node_3", node_data=node_data_3)

        # 2. Add edges to create connections for visualization
        edge_data_1 = EdgeData(
            source_uid="test_vis_node_1",
            target_uid="test_vis_node_2",
            description="Test Edge Description 1"
        )
        edge_data_2 = EdgeData(
            source_uid="test_vis_node_2",
            target_uid="test_vis_node_3",
            description="Test Edge Description 2"
        )
        self.kg.add_edge(edge_data=edge_data_1)
        self.kg.add_edge(edge_data=edge_data_2)

        # 3. Visualize the graph
        try:
            self.kg.visualize_graph(filename="test_graph.png")
        except Exception as e:
            raise ValueError(f"An error occurred during visualization: {e}") 

        # 4. Clean up (optional, depending on your test setup)
        self.kg.remove_edge(source_uid="test_vis_node_1", target_uid="test_vis_node_2")
        self.kg.remove_edge(source_uid="test_vis_node_2", target_uid="test_vis_node_3")
        self.kg.remove_node(node_uid="test_vis_node_1")
        self.kg.remove_node(node_uid="test_vis_node_2")
        self.kg.remove_node(node_uid="test_vis_node_3")


class FirestoreKGTests(_NoSQLKnowledgeGraphTests, unittest.TestCase):
    def create_kg_instance(self) -> NoSQLKnowledgeGraph:

        import os
        from dotenv import dotenv_values

        os.chdir(os.path.dirname(os.path.abspath(__file__))) 

        secrets = dotenv_values(".env")

        gcp_credential_file = str(secrets["GCP_CREDENTIAL_FILE"])
        project_id = str(secrets["GCP_PROJECT_ID"])
        database_id = str(secrets["FIRESTORE_DB_ID"])
        node_coll_id = str(secrets["NODE_COLL_ID"])
        edges_coll_id = str(secrets["EDGES_COLL_ID"])
        community_coll_id = str(secrets["COMM_COLL_ID"])

        fskg = FirestoreKG(
            gcp_project_id=project_id,
            gcp_credential_file=gcp_credential_file,
            firestore_db_id=database_id,
            node_collection_id=node_coll_id,
            edges_collection_id=edges_coll_id,
            community_collection_id=community_coll_id
        )
        return fskg

if __name__ == "__main__":
    unittest.main() 