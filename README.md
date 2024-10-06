# graph2nosql
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.x-blue.svg)](https://www.python.org/)


A simple Python interface to store and interact with knowledge graphs in your favourite NoSQL DB.

Knowledge Graphs are the up and coming tool to index knowlegde and make it understandable to your LLM applications. Working with Graph Databases are a pain though.

This Python interface aims to solve  this by offering a set of basic functions to store and manage your (knowledge graph) in your existing NoSQL DB. From experience Document based databases offer an exteremely attractive performance an price position comparing to some existing specialized databases. I found this to be attractive for simple graph storage use cases in which no fully structured query language is required.

This repository mostly caters own use and is not regularly updated or maintained.

## Implemented Databases for graph storage:
* [Firestore](https://firebase.google.com/docs/firestore)
* [MongoDB](https://www.mongodb.com/docs/)
* [Neo4J for latency & cost benchmark](https://neo4j.com/docs/)

## Performance Benchmark
Approximate latency performance benchmark comparing tool and technology. Benchmarking framework [can be found in `./benchmarks`](https://github.com/jakobap/graph2nosql/tree/main/benchmarks).

Values are processing seconds -> lower = better
| Feature | Firestore | MongoDB | Neo4j |
|---|---|---|---|
| Adding 100 Nodes | 3.03 | 2.51 | 1.91 |
| Query 100 individual nodes | 0.94 | 1.10 | 7.12 |
| Count 2nd degree connection of given node | 0.8 | tbd | 10.5 |
| Count 3rd degree connection of given node | 10.9 | tbd | 13.3 |

## Getting Started
`graph2nosql.py` is the abstract class defining the available operations.

1. Create an `.env` that stores your secrets & env vars.
2. Use a database object to interact with your nosql db.

### Initialize knowledge graph object
Every knowledge graph store object is a child of `NoSQLKnowledgeGraph` in `./base/operations.py`.

The graph contains three data objects: `NodeData`, `EdgeData` and `CommunityData`. Their respective attributes are defined in `./data_model/datamodel.py`.

```
from databases.firestore_kg import FirestoreKG

secrets = dotenv_values("../.env")
credentials, _ = google.auth.load_credentials_from_file(secrets["GCP_CREDENTIAL_FILE"])

fskg = FirestoreKG(gcp_credential_file=secret["GCP_CREDENTIAL_FILE"],
        gcp_project_id=str(secrets["GCP_PROJECT_ID"]),
        firestore_db_id=str(secrets["WIKIDATA_FS_DB"]),
        node_collection_id=str(secrets["NODE_COLL_ID"]),
        edges_collection_id=str(secrets["EDGES_COLL_ID"]),
        community_collection_id=str(secrets["COMM_COLL_ID"])
        )
```
### Add nodes
```
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

self.kg.add_node(node_uid="test_egde_node_1"node_data=node_data_1)
self.kg.add_node(node_uid="test_egde_node_2"node_data=node_data_2)
```

### Add directed and undirected edges
```
edge_data1 = EdgeData(
    source_uid="test_egde_node_1",
    target_uid="test_egde_node_2",
    description="This is a test egde description",
    directed=True
)

edge_data2 = EdgeData(
    source_uid="test_egde_node_3",
    target_uid="test_egde_node_2",
    description="This is a test egde description",
    directed=False
)

self.kg.add_edge(edge_data=edge_data1)
self.kg.add_edge(edge_data=edge_data2)
```


## Contributing
* If you decide to add new DB operations, please add corresponding tests to `graph2nosql_tests.py` 
* If you decide to write an implementation for another NoSQL db please make sure all tests in `graph2nosql_tests.py` succeed.

