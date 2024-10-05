"""MongoDB Database Operations"""

from typing import List

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from datamodel.data_model import NodeData, EdgeData, CommunityData
from base.operations import NoSQLKnowledgeGraph

import networkx as nx  # type: ignore


class MongoKG(NoSQLKnowledgeGraph):
    """MongoDB Database Operations Class"""

    def __init__(self,
                 mdb_uri: str,
                 mdb_db_id: str,
                 node_coll_id: str,
                 edges_coll_id: str,
                 community_collection_id: str
                 ):
        super().__init__()

        # Connect and send a ping to confirm a successful mongo db connection
        self.mdb_client = MongoClient(str(mdb_uri), server_api=ServerApi('1'))

        self.db = self.mdb_client[mdb_db_id]
        self.mdb_node_coll = self.db[node_coll_id]
        self.mdbe_edges_coll = self.db[edges_coll_id]
        self.mdb_comm_coll = self.db[community_collection_id]

        try:
            # client.admin.command('ping')
            self.mdb_client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)
            raise Exception(f"Error connecting to MongoDB: {e}")

    def add_node(self, node_uid: str, node_data: NodeData) -> None:
        """Adds an node to the knowledge graph."""
        try:
            # Check if a node with the same node_uid already exists
            if self.mdb_node_coll.find_one({"node_uid": node_uid}):
                raise ValueError(
                    f"Error: Node with node_uid '{node_uid}' already exists.")

            # Convert NodeData to a dictionary for MongoDB storage
            node_data_dict = node_data.__dict__

            # Insert the node data into the collection
            self.mdb_node_coll.insert_one(node_data_dict)

        except Exception as e:
            raise Exception(
                f"Error adding node with node_uid '{node_uid}': {e}") from e

    def get_node(self, node_uid: str) -> NodeData:
        """Retrieves an node from the knowledge graph."""
        # Find the node data based on node_uid
        node_data_dict = self.mdb_node_coll.find_one({"node_uid": node_uid})

        if node_data_dict:
            # Convert the dictionary back to a NodeData object
            return NodeData(
                node_uid=node_data_dict['node_uid'],
                node_title=node_data_dict['node_title'],
                node_type=node_data_dict['node_type'],
                node_description=node_data_dict['node_description'],
                node_degree=node_data_dict.get('node_degree', 0),
                document_id=node_data_dict.get('document_id', ''),
                community_id=node_data_dict.get('community_id', ''),
                edges_to=node_data_dict.get('edges_to', []),
                edges_from=node_data_dict.get('edges_from', []),
                embedding=node_data_dict.get('embedding', [])
            )
        else:
            raise KeyError(f"Error: No node found with node_uid: {node_uid}")

    def update_node(self, node_uid: str, node_data: NodeData) -> None:
        """Updates an existing node in the knowledge graph."""
        try:
            # Check if the node exists
            if not self.mdb_node_coll.find_one({"node_uid": node_uid}):
                raise KeyError(
                    f"Error: Node with node_uid '{node_uid}' does not exist.")

            # Convert NodeData to a dictionary for MongoDB storage
            node_data_dict = node_data.__dict__

            # Update the node data in the collection
            self.mdb_node_coll.update_one(
                {"node_uid": node_uid}, {"$set": node_data_dict}
            )

        except Exception as e:
            raise Exception(
                f"Error updating node with node_uid '{node_uid}': {e}") from e

    def remove_node(self, node_uid: str) -> None:
        """Removes a node from the knowledge graph."""

        # Check if the node exists
        if not self.node_exist(node_uid=node_uid):
            raise KeyError(
                f"Error: Node with node_uid '{node_uid}' does not exist.")

        # 1. Get the node data to find its connections
        node_data = self.get_node(node_uid)

        # TODO: Update edge collection on edge removal.

        # 2. Remove connections TO this node from other nodes
        for other_node_uid in node_data.edges_from:
            try:
                other_node_data = self.get_node(other_node_uid)
                other_node_data.edges_to = list(
                    edge for edge in other_node_data.edges_to if edge != node_uid
                )
                self.update_node(other_node_uid, other_node_data)
            except KeyError:
                # If the other node doesn't exist, just continue
                continue

        # 3. Remove connections FROM this node to other nodes
        for other_node_uid in node_data.edges_to:
            try:
                other_node_data = self.get_node(other_node_uid)
                other_node_data.edges_from = list(
                    edge for edge in other_node_data.edges_from if edge != node_uid
                )
                self.update_node(other_node_uid, other_node_data)
            except KeyError:
                # If the other node doesn't exist, just continue
                continue

        # 4. Finally, remove the node itself
        delete_result = self.mdb_node_coll.delete_one({"node_uid": node_uid})
        if delete_result.deleted_count == 1:
            return None
        else:
            raise KeyError(f"Error: No node found with node_uid: {node_uid}")

    def add_edge(self, edge_data: EdgeData) -> None:
        """Adds an edge (relationship) between two entities in the knowledge graph."""

        # TODO: consider moving to base class.

        # Check if source and target nodes exist
        if not self.node_exist(edge_data.source_uid):
            raise KeyError(
                f"Error: Source node with node_uid '{edge_data.source_uid}' does not exist.")
        if not self.node_exist(edge_data.target_uid):
            raise KeyError(
                f"Error: Target node with node_uid '{edge_data.target_uid}' does not exist.")

        # Type checking for edge_data
        if not isinstance(edge_data, EdgeData):
            raise TypeError(
                f"Error: edge_data must be of type EdgeData, not {type(edge_data)}")

        edge_uid = self._generate_edge_uid(
            edge_data.source_uid, edge_data.target_uid)

        try:
            source_node_data = self.get_node(edge_data.source_uid)
            target_node_data = self.get_node(edge_data.target_uid)

            source_node_data.edges_to = list(
                set(source_node_data.edges_to) | {edge_data.target_uid})
            self.update_node(edge_data.source_uid, source_node_data)

            # Add the edge to the target node's edges_from
            target_node_data.edges_from = list(
                set(target_node_data.edges_from) | {edge_data.source_uid})
            self.update_node(edge_data.target_uid, target_node_data)

            # Add the edge to the edges collection
            self._update_egde_coll(edge_uid=edge_uid,
                                   target_uid=edge_data.target_uid,
                                   source_uid=edge_data.source_uid,
                                   description=edge_data.description,
                                   directed=edge_data.directed)

            if not edge_data.directed:  # If undirected, add the reverse edge as well
                reverse_edge_uid = self._generate_edge_uid(
                    edge_data.target_uid, edge_data.source_uid)

                target_node_data.edges_to = list(
                    set(target_node_data.edges_to) | {edge_data.source_uid})
                self.update_node(edge_data.target_uid, target_node_data)

                # Since it's undirected, also add source_uid to target_node_data.edges_from
                source_node_data.edges_from = list(
                    set(source_node_data.edges_from) | {edge_data.target_uid})
                self.update_node(edge_data.source_uid, source_node_data)

                # Add the reverse edge to the edges collection
                self._update_egde_coll(edge_uid=reverse_edge_uid,
                                       target_uid=edge_data.source_uid,
                                       source_uid=edge_data.target_uid,
                                       description=edge_data.description,
                                       directed=edge_data.directed)

        except ValueError as e:
            raise ValueError(
                f"Error: Could not add edge from '{edge_data.source_uid}' to '{edge_data.target_uid}'. Details: {e}"
            ) from e

    def get_edge(self, source_uid: str, target_uid: str) -> EdgeData:
        """Retrieves an edge between two entities."""
        edge_uid = self._generate_edge_uid(source_uid, target_uid)
        edge_data_dict = self.mdbe_edges_coll.find_one({"edge_uid": edge_uid})

        if edge_data_dict:
            return EdgeData(
                edge_uid=edge_data_dict.get('edge_uid', ''),
                source_uid=edge_data_dict.get('source_uid', ''),
                target_uid=edge_data_dict.get('target_uid', ''),
                description=edge_data_dict.get('description', ''),
                directed=edge_data_dict.get('directed', True)
            )
        else:
            raise KeyError(f"Error: No edge found with edge_uid: {edge_uid}")

    def update_edge(self, edge_data: EdgeData) -> None:
        """Updates an existing edge in the knowledge graph."""

        # TODO: Consider moving to base

        # 1. Validate input and check if the edge exists
        if not isinstance(edge_data, EdgeData):
            raise TypeError(
                f"Error: edge_data must be of type EdgeData, not {type(edge_data)}")

        edge_uid = self._generate_edge_uid(
            edge_data.source_uid, edge_data.target_uid)

        if not self.edge_exist(source_uid=edge_data.source_uid, target_uid=edge_data.target_uid):
            raise KeyError(
                f"Error: Edge with edge_uid '{edge_uid}' does not exist.")

        # 2. Update the edge document in the EDGES collection
        try:
            self._update_egde_coll(
                edge_uid=edge_uid,
                target_uid=edge_data.target_uid,
                source_uid=edge_data.source_uid,
                description=edge_data.description,
                directed=edge_data.directed
            )
        except Exception as e:
            raise Exception(
                f"Error updating edge in edges collection: {e}") from e

        # 3. Update edge references in the NODES collection
        try:
            # 3a. Update source node
            source_node_data = self.get_node(edge_data.source_uid)
            # Ensure the target_uid is present in edges_to
            if edge_data.target_uid not in source_node_data.edges_to:
                source_node_data.edges_to = list(
                    set(source_node_data.edges_to) | {edge_data.target_uid})
                self.update_node(edge_data.source_uid, source_node_data)

            # 3b. Update target node
            target_node_data = self.get_node(edge_data.target_uid)
            # Ensure the source_uid is present in edges_from
            if edge_data.source_uid not in target_node_data.edges_from:
                target_node_data.edges_from = list(
                    set(target_node_data.edges_from) | {edge_data.source_uid})
                self.update_node(edge_data.target_uid, target_node_data)

        except Exception as e:
            raise Exception(
                f"Error updating edge references in nodes: {e}") from e

    def remove_edge(self, source_uid: str, target_uid: str) -> None:
        """Removes an edge between two entities."""

        # Get involved edge and node data
        try:
            edge_data = self.get_edge(
                source_uid=source_uid, target_uid=target_uid)
        except Exception as e:
            raise Exception(f"Error getting edge: {e}") from e

        try:
            source_node_data = self.get_node(node_uid=source_uid)
        except Exception as e:
            raise Exception(f"Error getting source node: {e}") from e

        try:
            target_node_data = self.get_node(node_uid=target_uid)
        except Exception as e:
            raise Exception(f"Error getting target node: {e}") from e

        # remove target_uid from from source -> target
        try:
            source_node_data.edges_to.remove(target_uid)
            self.update_node(source_uid, source_node_data)
        except ValueError as e:
            raise ValueError(
                f"Error: Target node not in source's edges_to: {e}")

        # remove source_uid from target <- source
        try:
            target_node_data.edges_from.remove(source_uid)
            self.update_node(target_uid, target_node_data)
        except ValueError as e:
            raise ValueError(
                f"Error: Source node not in target's edges_to: {e}")

        # Remove the edge from the edges collection
        edge_uid = self._generate_edge_uid(source_uid, target_uid)
        delete_result = self.mdbe_edges_coll.delete_one({"edge_uid": edge_uid})
        if delete_result.deleted_count == 0:
            raise KeyError(
                f"Error: No edge found with source_uid '{source_uid}' and target_uid '{target_uid}'")

        # remove the opposite direction if edge undirected
        if not edge_data.directed:
            # remove target_uid from source <- target
            try:
                source_node_data.edges_from.remove(target_uid)
                self.update_node(source_uid, source_node_data)
            except ValueError as e:
                raise ValueError(
                    f"Error: Target node not in source's edges_to: {e}")

            # remove source_uid from target -> source
            try:
                target_node_data.edges_to.remove(source_uid)
                self.update_node(target_uid, target_node_data)
            except ValueError as e:
                raise ValueError(
                    f"Error: Source node not in target's edges_to: {e}")

            # Remove the edge from the edges collection
            reverse_edge_uid = self._generate_edge_uid(target_uid, source_uid)
            delete_result = self.mdbe_edges_coll.delete_one(
                {"edge_uid": reverse_edge_uid})
            if delete_result.deleted_count == 0:
                raise KeyError(
                    f"Error: No reverse edge found with source_uid '{target_uid}' and target_uid '{source_uid}'")

        else:
            pass

    def build_networkx(self) -> None:
        """Builds the NetworkX representation of the full graph.
        https://networkx.org/documentation/stable/index.html
        """
        graph = nx.Graph()  # Initialize an undirected NetworkX graph

        # 1. Add Nodes to the NetworkX Graph
        for node in self.mdb_node_coll.find():
            graph.add_node(node['node_uid'], **node)

        # 2. Add Edges to the NetworkX Graph
        for edge in self.mdbe_edges_coll.find():
            source_uid = edge['source_uid']
            target_uid = edge['target_uid']
            graph.add_edge(source_uid, target_uid)

        self.networkx = graph

    def store_community(self, community: CommunityData) -> None:
        """Takes valid graph community data and upserts the database with it.
        https://www.nature.com/articles/s41598-019-41695-z
        """
        pass

    def _generate_edge_uid(self, source_uid: str, target_uid: str):
        return f"{source_uid}_to_{target_uid}"

    def _update_egde_coll(self, edge_uid: str, source_uid: str,
                          target_uid: str, description: str, directed: bool) -> None:
        """Update edge record in the edges collection."""
        edge_data_dict = {
            "edge_uid": edge_uid,
            "source_uid": source_uid,
            "target_uid": target_uid,
            "description": description,
            "directed": directed
        }
        self.mdbe_edges_coll.update_one(
            {"edge_uid": edge_uid}, {"$set": edge_data_dict}, upsert=True
        )
        return None

    def get_nearest_neighbors(self, query_vec) -> List[str]:
        """Implements nearest neighbor search based on nosql db index."""
        pass

    def get_community(self, community_id: str) -> CommunityData:
        """Retrieves the community report for a given community id."""
        return

    def list_communities(self) -> List[CommunityData]:
        """Lists all stored communities for the given network."""
        return

    def clean_zerodegree_nodes(self) -> None:
        """Removes all nodes with degree 0."""
        return

    def edge_exist(self, source_uid: str, target_uid: str) -> bool:
        """Checks for edge existence and returns boolean"""
        edge_uid = self._generate_edge_uid(source_uid, target_uid)
        if self.mdbe_edges_coll.find_one({"edge_uid": edge_uid}) is not None:
            return True
        return False

    def node_exist(self, node_uid: str) -> bool:
        """Checks for node existence and returns boolean"""
        if self.mdb_node_coll.find_one({"node_uid": node_uid}) is not None:
            return True
        else:
            return False

    def flush_kg(self) -> None:
        """Method to wipe the complete datastore of the knowledge graph"""
        try:
            # Drop the node collection
            self.mdb_node_coll.drop()

            # Drop the edges collection
            self.mdbe_edges_coll.drop()

            # Drop the community collection
            self.mdb_comm_coll.drop()

        except Exception as e:
            raise Exception(f"Error flushing MongoDB collections: {e}") from e


if __name__ == "__main__":
    import os
    from dotenv import dotenv_values

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    secrets = dotenv_values("../.env")

    mdb_username = str(secrets["MDB_USERNAME"])
    mdb_passowrd = str(secrets["MDB_PASSWORD"])
    mdb_cluster = str(secrets["MDB_CLUSTER"])

    uri = f"mongodb+srv://{mdb_username}:{mdb_passowrd}@cluster0.pjx3w.mongodb.net/?retryWrites=true&w=majority&appName={mdb_cluster}"

    mkg = MongoKG(
        mdb_uri=uri,
        mdb_db_id=str(secrets["MDB_DB_ID"]),
        node_coll_id=str(secrets["NODE_COLL_ID"]),
        edges_coll_id=str(secrets["EDGES_COLL_ID"]),
        community_collection_id=str(secrets["COMM_COLL_ID"])
    )

    # node = mkg.get_node(node_uid="2022 IRANIAN PROTESTS")

    print("helLO wOrLD!")
