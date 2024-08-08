try:
    from nosql_kg.data_model import NodeData, EdgeData, CommunityData, NodeEmbeddings
except:
    from data_model import NodeData, EdgeData, CommunityData, NodeEmbeddings

from re import A
import networkx

from abc import ABC, abstractmethod
from typing import Dict, List
import datetime

import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import graspologic as gc


class NoSQLKnowledgeGraph(ABC):
    """
    Base Class for storing and interacting with the KG and manages data model.
    """
    networkx: nx.Graph | nx.DiGraph = nx.Graph(
    )  # networkx representation of graph in nosqldb

    @abstractmethod
    def add_node(self, node_uid: str, node_data: NodeData) -> None:
        """Adds an node to the knowledge graph."""
        pass

    @abstractmethod
    def get_node(self, node_uid: str) -> NodeData:
        """Retrieves an node from the knowledge graph."""
        pass

    @abstractmethod
    def update_node(self, node_uid: str, node_data: NodeData) -> None:
        """Updates an existing node in the knowledge graph."""
        pass

    @abstractmethod
    def remove_node(self, node_uid: str) -> None:
        """Removes an node from the knowledge graph."""
        pass

    @abstractmethod
    def add_edge(self, edge_data: EdgeData, directed: bool = True) -> None:
        """Adds an edge (relationship) between two entities in the knowledge graph."""
        pass

    @abstractmethod
    def get_edge(self, source_uid: str, target_uid: str) -> EdgeData:
        """Retrieves an edge between two entities."""
        pass

    @abstractmethod
    def update_edge(self, edge_data: EdgeData) -> None:
        """Updates an existing edge in the knowledge graph."""
        pass

    @abstractmethod
    def remove_edge(self, source_uid: str, target_uid: str) -> None:
        """Removes an edge between two entities."""
        pass

    @abstractmethod
    def build_networkx(self) -> None:
        """Builds the NetworkX representation of the full graph.
        https://networkx.org/documentation/stable/index.html
        """
        pass

    @abstractmethod
    def store_community(self, community: CommunityData) -> None:
        """Takes valid graph community data and upserts the database with it.
        https://www.nature.com/articles/s41598-019-41695-z
        """
        pass

    @abstractmethod
    def _generate_edge_uid(self, source_uid: str, target_uid: str) -> str:
        """Generates Edge uid for the network based on source and target nod uid"""
        return ""

    @abstractmethod
    def get_nearest_neighbors(self, query_vec) -> List[str]:
        """Implements nearest neighbor search based on nosql db index."""
        pass

    @abstractmethod
    def get_community(self, community_id: str) -> CommunityData: 
        """Retrieves the community report for a given community id."""
        pass

    @abstractmethod
    def list_communities(self) -> List[CommunityData]:
        """Lists all stored communities for the given network.""" 
        pass

    def visualize_graph(self, filename: str = f"graph_{datetime.datetime.now()}.png") -> None:
        """Visualizes the provided networkx graph using matplotlib.

        Args:
            graph (nx.Graph): The graph to visualize.
        """
        self.build_networkx()

        if self.networkx is not None:
            # Create a larger figure for better visualization
            plt.figure(figsize=(12, 12))

            # Use a spring layout for a more visually appealing arrangement
            pos = nx.spring_layout(self.networkx, k=0.3, iterations=50)

            # Draw nodes with different colors based on entity type
            entity_types = set(data["node_type"]
                               for _, data in self.networkx.nodes(data=True))
            color_map = plt.cm.get_cmap("tab10", len(entity_types))
            for i, entity_type in enumerate(entity_types):
                nodes = [n for n, d in self.networkx.nodes(
                    data=True) if d["node_type"] == entity_type]
                nx.draw_networkx_nodes(
                    self.networkx,
                    pos,
                    nodelist=nodes,
                    node_color=[color_map(i)],  # type: ignore
                    label=entity_type,
                    # type: ignore
                    node_size=[
                        10 + 50 * self.networkx.degree(n) for n in nodes] # type: ignore
                )

            # Draw edges with labels
            nx.draw_networkx_edges(self.networkx, pos, width=0.5, alpha=0.5)
            # edge_labels = nx.get_edge_attributes(graph, "description")
            # nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels, font_size=6)

            # Add node labels with descriptions
            node_labels = {
                node: node
                for node, data in self.networkx.nodes(data=True)
            }
            nx.draw_networkx_labels(
                self.networkx, pos, labels=node_labels, font_size=8)

            plt.title("Extracted Knowledge Graph")
            plt.axis("off")  # Turn off the axis

            # Add a legend for node colors
            plt.legend(handles=[Line2D([0], [0], marker='o', color='w', label=entity_type,
                                       markersize=10, markerfacecolor=color_map(i)) for i, entity_type in enumerate(entity_types)])

            plt.savefig(filename)

        else:
            raise ValueError(
                "Error: NetworkX graph is not initialized. Call build_networkx() first.")

    def get_louvain_communities(self) -> list:
        """Computes and returns all Louvain communities for the given network.
        https://www.nature.com/articles/s41598-019-41695-z

        Sample Output:
        [{'"2023 NOBEL PEACE PRIZE"'}, {'"ANDREI SAKHAROV PRIZE"'},
        {'"ANDREI SAKHAROV"'}, {'"ENCIEH ERFANI"', '"INSTITUTE FOR ADVANCED STUDIES IN BASIC SCIENCES IN ZANJAN, IRAN"'}]
        """
        # 1. Build (or update) the NetworkX graph
        self.build_networkx()

        # 2. Apply Louvain algorithm
        if self.networkx is not None:
            louvain_comm_list = nx.algorithms.community.louvain_communities(
                self.networkx)
            return louvain_comm_list  # type: ignore
        else:
            raise ValueError(
                "Error: NetworkX graph is not initialized. Call build_networkx() first.")

    def get_node2vec_embeddings(
        self,
        dimensions: int = 768,
        num_walks: int = 10,
        walk_length: int = 40,
        window_size: int = 2,
        iterations: int = 3,
        random_seed: int = 69
        ) -> NodeEmbeddings:
        """Generate node embeddings using Node2Vec."""
        
        # update networkx representation of graph
        self.build_networkx()
        
        # generate embedding
        lcc_tensors = gc.embed.node2vec_embed(  # type: ignore
            graph=self.networkx,
            dimensions=dimensions,
            window_size=window_size,
            iterations=iterations,
            num_walks=num_walks,
            walk_length=walk_length,
            random_seed=random_seed,
        )
        return NodeEmbeddings(embeddings=lcc_tensors[0], nodes=lcc_tensors[1])


if __name__ == "__main__":
    print("Hello World!")
