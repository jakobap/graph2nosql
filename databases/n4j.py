from ..graph2nosql.graph2nosql import NoSQLKnowledgeGraph
from ..datamodel.data_model import NodeData, EdgeData, CommunityData


class AtlasKG(NoSQLKnowledgeGraph):
    """
    Base Class for storing and interacting with the KG and manages data model.
    """
    networkx: nx.Graph | nx.DiGraph = nx.Graph(
    )  # networkx representation of graph in nosqldb

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