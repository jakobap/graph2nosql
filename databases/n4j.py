from base.operations import NoSQLKnowledgeGraph
from datamodel.data_model import NodeData, EdgeData, CommunityData

import dotenv
import os
from typing import Dict, List

from neo4j import GraphDatabase
import networkx as nx  # type: ignore


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

    def add_node(self, node_uid: str, node_data: NodeData) -> None:
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
        
            print("Created {nodes_created} nodes with if {node_uid} in {time} ms.".format(
                nodes_created=summary.counters.nodes_created,
                node_uid=node_uid,
                time=summary.result_available_after
            ))
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
                    node_degree=node_data.get('node_degree'),
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

    def remove_node(self, node_uid: str) -> None:
        """Removes a node from the knowledge graph."""

        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()

            summary = driver.execute_query(
                "MATCH (n {node_uid: $node_uid}) DETACH DELETE n",
                node_uid=node_uid
            ).summary

            if summary.counters.nodes_deleted == 0:
                raise KeyError(f"Error: No node found with node_uid: {node_uid}")
        return None

    def add_edge(self, edge_data: EdgeData, directed: bool = True) -> None:
        """Adds an edge (relationship) between two entities in the knowledge graph."""

        # get source and target node data
        source_node_data = self.get_node(edge_data.source_uid)
        target_node_data = self.get_node(edge_data.target_uid)

        # update source and target node data
        source_node_data.edges_to = list(set(source_node_data.edges_to) | {edge_data.target_uid})
        self.update_node(edge_data.source_uid, source_node_data)
        target_node_data.edges_from = list(set(target_node_data.edges_from) | {edge_data.source_uid})
        self.update_node(edge_data.target_uid, target_node_data)

        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()

            if directed:
                query = """
                MATCH (source:""" + source_node_data.node_type + """ {node_uid: $source_uid}), (target:""" + target_node_data.node_type + """ {node_uid: $target_uid})
                CREATE (source)-[:DIRECTED {description: $description}]->(target)
                """

            elif not directed:
                query = """
                MATCH (source:""" + source_node_data.node_type + """ {node_uid: $source_uid}), (target:""" + target_node_data.node_type + """ {node_uid: $target_uid})
                CREATE (source)-[:UNDIRECTED {description: $description}]->(target), (target)-[:UNDIRECTED {description: $description}]->(source)
                """

                # Since it's undirected, also add source_uid to target_node_data.edges_from and vice versa
                target_node_data.edges_to = list(set(target_node_data.edges_to) | {edge_data.source_uid})
                self.update_node(edge_data.target_uid, target_node_data)
                source_node_data.edges_from = list(set(source_node_data.edges_from) | {edge_data.target_uid})
                self.update_node(edge_data.source_uid, source_node_data)

            summary = driver.execute_query(
                query,
                source_uid=edge_data.source_uid,
                target_uid=edge_data.target_uid,
                description=edge_data.description
            ).summary
            
            print("#### Created {count} egdes {origin} -> {target} egdes in {time} ms.".format(
                count=str(summary.counters.relationships_created),
                origin=str(edge_data.source_uid),
                target=str(edge_data.target_uid),
                time=str(summary.result_available_after)
                ))

        return None

    def get_edge(self, source_uid: str, target_uid: str) -> EdgeData:
        """Retrieves an edge between two entities."""

        # get source and target node data
        source_node_data = self.get_node(source_uid)
        target_node_data = self.get_node(target_uid)

        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()

            # Use parameters for source_uid and target_uid
            records, summary, keys = driver.execute_query(
                """
                MATCH (source:""" + source_node_data.node_type + """ {node_uid: $source_uid})-[r]->(target:""" + target_node_data.node_type + """ {node_uid: $target_uid}) 
                RETURN r
                """,
                source_uid=source_uid,
                target_uid=target_uid
            )

            if records:
                record = records[0][0]
                edge_type = record.type
                description = record.get('description')
                return EdgeData(source_uid=source_uid, target_uid=target_uid, description=description, edge_uid=self._generate_edge_uid(source_uid, target_uid))
            else:
                raise KeyError(f"Error: No edge found between source_uid: '{source_uid}' and target_uid: '{target_uid}'")

    def update_edge(self, edge_data: EdgeData) -> None:
        """Updates an existing edge in the knowledge graph."""

        # get source and target node data
        source_node_data = self.get_node(edge_data.source_uid)
        target_node_data = self.get_node(edge_data.target_uid)

        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()

            # Use parameters for all properties in the Cypher query
            summary = driver.execute_query(
                """
                MATCH (source:""" + source_node_data.node_type + """ {node_uid: $source_uid})-[r]->(target:""" + target_node_data.node_type + """ {node_uid: $target_uid})
                SET r.description = $description
                RETURN r
                """,
                source_uid=edge_data.source_uid,
                target_uid=edge_data.target_uid,
                description=edge_data.description
            ).summary
        return None

    def remove_edge(self, source_uid: str, target_uid: str) -> None:
        """Removes an edge between two entities."""

        try: 
            # Get source and target node data (this will raise KeyError if not found)
            source_node_data = self.get_node(source_uid)
            target_node_data = self.get_node(target_uid)

            with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
                driver.verify_connectivity()

                # Remove edge from source to target
                summary = driver.execute_query(
                    """
                    MATCH (source:""" + source_node_data.node_type + """ {node_uid: $source_uid})-[r]->(target:""" + target_node_data.node_type + """ {node_uid: $target_uid})
                    DELETE r
                    """,
                    source_uid=source_uid,
                    target_uid=target_uid
                ).summary

                # Optionally, you might want to check if the edge was actually deleted
                if summary.counters.relationships_deleted == 0:
                    raise KeyError(f"Error: No edge found between source_uid: '{source_uid}' and target_uid: '{target_uid}'")
        except KeyError:
            empty_node = NodeData(
                node_uid="",
                node_title="",
                node_type="",
                node_description="",
                node_degree=0,
                document_id="",
            )
            source_node_data = empty_node
            target_node_data = empty_node

        # Update the node data to reflect the removed edge
        try:
            source_node_data.edges_to.remove(target_uid)
            self.update_node(source_uid, source_node_data)
        except ValueError:
            pass  # Target node not in source's edges_to, likely due to a directed edge

        try:
            target_node_data.edges_from.remove(source_uid)
            self.update_node(target_uid, target_node_data)
        except ValueError:
            pass  # Source node not in target's edges_from, likely due to a directed edge
        return None

    def build_networkx(self) -> nx.Graph:
        """Builds the NetworkX representation of the full graph.
        https://networkx.org/documentation/stable/index.html
        """
        graph = nx.Graph()  # Initialize an undirected NetworkX graph

        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()

            # 1. Fetch all nodes and their properties
            records, summary, keys = driver.execute_query("MATCH (n) RETURN n")
            
            # Check if any records were returned
            if records:
                for record in records:
                    node = record["n"]
                    node_data = {
                        "node_uid": node.get("node_uid"),
                        "node_title": node.get("node_title"),
                        "node_type": node.get("node_type"),
                        "node_description": node.get("node_description"),
                        "node_degree": node.get("node_degree"),
                        "document_id": node.get("document_id"),
                        "edges_to": node.get("edges_to", []),
                        "edges_from": node.get("edges_from", []),
                        "embedding": node.get("embedding", [])
                    }
                    graph.add_node(node.get("node_uid"), **node_data)

                # 2. Fetch all relationships and add edges to the graph
                records, summary, keys = driver.execute_query("MATCH (source)-[r]->(target) RETURN source, r, target")
                for record in records:
                    source_uid = record["source"]["node_uid"]
                    target_uid = record["target"]["node_uid"]
                    # Add edge attributes if needed (e.g., 'description' from 'r')
                    graph.add_edge(source_uid, target_uid)
            else:
                print("Warning: No nodes found in the database. Returning an empty NetworkX graph.")

        self.networkx = graph
        return graph

    def store_community(self, community: CommunityData) -> None:
        """Takes valid graph community data and upserts the database with it.
        https://www.nature.com/articles/s41598-019-41695-z
        """
        pass

    def _generate_edge_uid(self, source_uid: str, target_uid: str) -> str:
        """Generates Edge uid for the network based on source and target nod uid"""
        return f"{source_uid}_to_{target_uid}"

    def edge_exist(self, source_uid: str, target_uid: str) -> bool:
        """Checks for edge existence and returns boolean"""
        try:
            # Try to retrieve the edge
            self.get_edge(source_uid, target_uid)
            return True  # Edge exists
        except KeyError:
            return False  # Edge does not exist

    def node_exist(self, node_uid: str) -> bool:
        """Checks for node existence and returns boolean"""
        try:
            # Try to retrieve the node
            self.get_node(node_uid)
            return True  # Node exists
        except KeyError:
            return False  # Node does not exist

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

    def flush_kg(self) -> None:
        """Method to wipe the complete datastore of the knowledge graph"""
        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            driver.verify_connectivity()
            summary = driver.execute_query(
                """
                MATCH (n) 
                DETACH DELETE n
                """
            ).summary
        return None


if __name__ == "__main__":

    load_status = dotenv.load_dotenv("Neo4j-39cb28f0-Created-2024-09-23.txt")
    if load_status is False:
        raise RuntimeError('Environment variables not loaded.')

    URI = str(os.getenv("NEO4J_URI"))
    AUTH = (str(os.getenv("NEO4J_USERNAME")), str(os.getenv("NEO4J_PASSWORD")))

    aura = AuraKG(uri=URI, auth=AUTH)

    # aura.add_node(NodeData(node_uid="test_uid_2", node_title="test2", node_type="test", node_description="test", node_degree=0, document_id="doc test"))

    print(aura.get_node("test_uid"))
    print(aura.get_node("test_uid_2"))

    print("Hello World!")
