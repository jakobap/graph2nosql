from dataclasses import dataclass, field
from typing import Tuple
import numpy as np


@dataclass
class EdgeData:
    source_uid: str
    target_uid: str 
    description: str
    edge_uid: str | None = None
    document_id: str | None = None


@dataclass
class NodeData:
    node_uid: str
    node_title: str
    node_type: str
    node_description: str
    node_degree: int
    document_id: str # identifier for source knowlede base document for this entity
    community_id: int | None = None # community id based on source document 
    edges_to: list[str] = field(default_factory=list)
    edges_from: list[str] = field(default_factory=list)  # in case of directed graph
    embedding: list[float] = field(default_factory=list)  # text embedding representing node e.g. combination of title & description


@dataclass
class CommunityData:
    title: str # title of comm, None if not yet computed
    community_nodes: set[str] = field(default_factory=set) # list of node_uid belonging to community
    summary: str | None = None # description of comm, None if not yet computed
    document_id: str | None = None # identifier for source knowlede base document for this entity
    community_uid: str | None = None # community identifier
    community_embedding: Tuple[float, ...] = field(default_factory=tuple) # text embedding representing community
    rating: int | None = None
    rating_explanation: str | None = None
    findings: list[dict] | None = None

    def __to_dict__(self):
        """Converts the CommunityData instance to a dictionary."""
        return {
            "title": self.title,
            "community_nodes": list(self.community_nodes),  # Convert set to list for JSON serialization
            "summary": self.summary,
            "document_id": self.document_id,
            "community_uid": self.community_uid,
            "community_embedding": list(self.community_embedding),  # Convert tuple to list for JSON serialization
            "rating": self.rating,
            "rating_explanation": self.rating_explanation,
            "findings": self.findings
        }

    @classmethod
    def __from_dict__(cls, data: dict):
        """Creates a CommunityData instance from a dictionary."""
        return cls(
            title=data.get("title") or "",
            community_nodes=set(data.get("community_nodes", [])),  # Convert list to set
            summary=data.get("summary"),
            document_id=data.get("document_id"),
            community_uid=data.get("community_uid"),
            community_embedding=tuple(data.get("community_embedding", [])),  # Convert list to tuple
            rating=data.get("rating"),
            rating_explanation=data.get("rating_explanation"),
            findings=data.get("findings")
        )



@dataclass
class NodeEmbeddings:
    """Node embeddings class definition."""

    nodes: list[str]
    embeddings: np.ndarray