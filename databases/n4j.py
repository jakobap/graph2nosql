from graph2nosql.graph2nosql import NoSQLKnowledgeGraph
from datamodel.data_model import NodeData, EdgeData, CommunityData

from neo4j import GraphDatabase

import dotenv
import os
from typing import Dict, List


class AuraKG(NoSQLKnowledgeGraph):
    """
    Base Class for storing and interacting with the KG and manages data model.
    """
    def __init__(self,
                 uri: str,
                 auth: tuple[str,str]
                 ):
        super().__init__()
        self.uri = uri
        self.auth = auth

    def add_node(self, node_data: NodeData) -> None:
        """Adds an node to the knowledge graph."""

        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()
            print("Connection established.")

            summary = driver.execute_query(
                "CREATE (:" + node_data.node_type + " { "
                "node_uid: $node_uid, "
                "node_title: $node_title, "
                "node_type: $node_type, "
                "node_description: $node_description, "
                "node_degree: $node_degree, "
                "document_id: $document_id, "
                "community_id: $community_id, "
                "edges_to: $edges_to, "
                "edges_from: $edges_from, "
                "embedding: $embedding "
                "})",
                node_uid=node_data.node_uid,
                node_title=node_data.node_title,
                node_type=node_data.node_type,
                node_description=node_data.node_description,
                node_degree=node_data.node_degree,
                document_id=node_data.document_id,
                community_id=node_data.community_id,
                edges_to=node_data.edges_to,
                edges_from=node_data.edges_from,
                embedding=node_data.embedding
            ).summary
        
            # print("Created {nodes_created} nodes in {time} ms.".format(
            #     nodes_created=summary.counters.nodes_created,
            #     time=summary.result_available_after
            # ))
        return None

    def get_node(self, node_uid: str) -> NodeData:
        """Retrieves a node from the knowledge graph."""

        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()

            # Use a parameter for node_uid in the Cypher query
            records, summary, keys = driver.execute_query(
                "MATCH (n {node_uid: $node_uid}) RETURN n",
                node_uid=node_uid  # Pass node_uid as a parameter
            )

            if records:  # Check if any records were returned
                record = records[0]  # Get the first record
                node_data = record['n']
                # Convert Neo4j node properties to NodeData object
                return NodeData(
                    node_uid=node_data.get('node_uid'),  # Assuming node_uid is a property
                    node_title=node_data.get('node_title'), 
                    node_type=node_data.get('node_type'),
                    node_description=node_data.get('node_description'),
                    node_degree=len(node_data.get('edges_to', [])) + len(node_data.get('edges_from', [])),
                    document_id=node_data.get('document_id'),
                    edges_to=node_data.get('edges_to', []),
                    edges_from=node_data.get('edges_from', []),
                    embedding=node_data.get('embedding', [])
                )
            else:
                raise KeyError(f"Error: No node found with node_uid: {node_uid}")

    def update_node(self, node_uid: str, node_data: NodeData) -> None:
        """Updates an existing node in the knowledge graph."""

        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()

            # Use parameters for all properties in the Cypher query
            summary = driver.execute_query(
                """
                MATCH (n { node_uid: $node_uid })
                SET n.node_title = $node_title,
                    n.node_type = $node_type,
                    n.node_description = $node_description,
                    n.node_degree = $node_degree,
                    n.document_id = $document_id,
                    n.community_id = $community_id,
                    n.edges_to = $edges_to,
                    n.edges_from = $edges_from,
                    n.embedding = $embedding
                RETURN n
                """,
                node_uid=node_uid,
                node_title=node_data.node_title,
                node_type=node_data.node_type,
                node_description=node_data.node_description,
                node_degree=node_data.node_degree,
                document_id=node_data.document_id,
                community_id=node_data.community_id,
                edges_to=node_data.edges_to,
                edges_from=node_data.edges_from,
                embedding=node_data.embedding
            ).summary

            if summary.counters.nodes_updated == 0:
                raise KeyError(f"Error: No node found with node_uid: {node_uid}")


    def remove_node(self, node_uid: str) -> None:
        """Removes an node from the knowledge graph."""
        pass

    def add_edge(self, edge_data: EdgeData, directed: bool = True) -> None:
        """Adds an edge (relationship) between two entities in the knowledge graph."""
        pass

    def get_edge(self, source_uid: str, target_uid: str) -> EdgeData:
        """Retrieves an edge between two entities."""
        pass

    def update_edge(self, edge_data: EdgeData) -> None:
        """Updates an existing edge in the knowledge graph."""
        pass

    def remove_edge(self, source_uid: str, target_uid: str) -> None:
        """Removes an edge between two entities."""
        pass

    def build_networkx(self) -> None:
        """Builds the NetworkX representation of the full graph.
        https://networkx.org/documentation/stable/index.html
        """
        pass

    def store_community(self, community: CommunityData) -> None:
        """Takes valid graph community data and upserts the database with it.
        https://www.nature.com/articles/s41598-019-41695-z
        """
        pass

    def _generate_edge_uid(self, source_uid: str, target_uid: str) -> str:
        """Generates Edge uid for the network based on source and target nod uid"""
        return ""

    def get_nearest_neighbors(self, query_vec) -> List[str]:
        """Implements nearest neighbor search based on nosql db index."""
        pass

    def get_community(self, community_id: str) -> CommunityData: 
        """Retrieves the community report for a given community id."""
        pass

    def list_communities(self) -> List[CommunityData]:
        """Lists all stored communities for the given network.""" 
        pass

    def clean_zerodegree_nodes(self) -> None:
        """Removes all nodes with degree 0."""
        pass


if __name__ == "__main__":

    load_status = dotenv.load_dotenv("Neo4j-39cb28f0-Created-2024-09-23.txt")
    if load_status is False:
        raise RuntimeError('Environment variables not loaded.')

    URI = os.getenv("NEO4J_URI")
    AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

    aura = AuraKG(uri=URI, auth=AUTH)

    # aura.add_node(NodeData(node_uid="test_uid_2", node_title="test2", node_type="test", node_description="test", node_degree=0, document_id="doc test"))

    print(aura.get_node("test_uid"))
    print(aura.get_node("test_uid_2"))

    print("Hello World!")
