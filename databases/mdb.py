from datamodel.data_model import NodeData, EdgeData, CommunityData, NodeEmbeddings
from base.operations import NoSQLKnowledgeGraph

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from re import A
from typing import Dict, List
import datetime

import networkx as nx  # type: ignore
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import graspologic as gc


class MongoKG(NoSQLKnowledgeGraph):
    def __init__(self,
                 mdb_uri: str):
        super().__init__()

        # Connect and send a ping to confirm a successful mongo db connection
        self.mdb_client = MongoClient(str(mdb_uri), server_api=ServerApi('1'))
        try:
            # client.admin.command('ping')
            self.mdb_client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)
            raise Exception(f"Error connecting to MongoDB: {e}")

    def add_node(self, node_uid: str, node_data: NodeData) -> None:
        """Adds an node to the knowledge graph."""
        pass

    def get_node(self, node_uid: str) -> NodeData:
        """Retrieves an node from the knowledge graph."""
        pass

    def update_node(self, node_uid: str, node_data: NodeData) -> None:
        """Updates an existing node in the knowledge graph."""
        pass

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

    def edge_exist(self, source_uid: str, target_uid: str) -> bool:
        """Checks for edge existence and returns boolean"""
        pass

    def node_exist(self, node_uid: str) -> bool:
        """Checks for node existence and returns boolean"""
        pass

    def flush_kg(self) -> None:
        """Method to wipe the complete datastore of the knowledge graph"""
        pass


if __name__ == "__main__":
    import os
    from dotenv import dotenv_values

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    secrets = dotenv_values("../.env")

    mdb_username = str(secrets["MDB_USERNAME"])
    mdb_passowrd = str(secrets["MDB_PASSWORD"])
    mdb_cluster = str(secrets["MDB_CLUSTER"])

    # uri = "mongodb+srv://poerschmann:<db_password>@cluster0.pjx3w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    # uri ="mongodb+srv://poerschmann:<db_password>@cluster0.pjx3w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    uri = f"mongodb+srv://{mdb_username}:{mdb_passowrd}@cluster0.pjx3w.mongodb.net/?retryWrites=true&w=majority&appName={mdb_cluster}"
    
    mkg = MongoKG(
        mdb_uri=uri
    )

    node = mkg.get_node(node_uid="2022 IRANIAN PROTESTS")

    print("helLO wOrLD!")
