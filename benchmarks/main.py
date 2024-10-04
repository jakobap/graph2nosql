from google.cloud import bigquery
import google.auth 

import os
import json
from dotenv import dotenv_values
import time
from abc import ABC, abstractmethod
from typing import Any

from base.operations import NoSQLKnowledgeGraph
from databases.firestore_kg import FirestoreKG
from databases.n4j import AuraKG
from datamodel.data_model import NodeData, EdgeData, CommunityData


class KGDBBenchmarkDataFetcher:
    def __init__(self):
        pass


class BigQueryDataFetcher:
    def __init__(self):
        pass


class KGDBBenchmark(ABC):
    def __init__(self,
                 benchmark_name: str,
                 option_1: NoSQLKnowledgeGraph,
                 option_2: NoSQLKnowledgeGraph,
                 import_lim: int,
                 option_1_name: str = "",
                 option_2_name: str = "",
                 ):
        self.benchmark_name = benchmark_name
        self.option_1 = option_1
        self.option_2 = option_2
        self.option_1_name = option_1_name
        self.option_2_name = option_2_name
        self.option_1_time = 0
        self.option_2_time = 0
        self.import_lim = import_lim

    def __call__(self, records):

        print(
            f'$$$$ Starting Benchmark {self.option_1_name} vs. {self.option_2_name} $$$$')

        for row in records:
            data = self._construct_data(row)

            # Option 1
            start_time_1 = time.time()
            try:
                self._db_transaction(kgdb=self.option_1,
                                     data=data, option_name=self.option_1_name)
            except:
                pass
            end_time_2 = time.time()
            self.option_1_time += (end_time_2 - start_time_1)

            # Option 2
            start_time_2 = time.time()
            try:
                self._db_transaction(kgdb=self.option_2,
                                     data=data, option_name=self.option_2_name)
            except:
                pass
            end_time_2 = time.time()
            self.option_2_time += (end_time_2 - start_time_2)

        self._benchmark_reporting()
        print("hEllO wOrlD!")

    def _benchmark_reporting(self) -> None:
        print(f'{self.option_1_name} time for {self.import_lim} {self.benchmark_name}: {self.option_1_time}')
        print(f'{self.option_2_name} time for {self.import_lim} {self.benchmark_name}: {self.option_2_time}')
        return None

    @abstractmethod
    def _construct_data(self, row: Any):
        # constructs the data to be used for the benchmark db transaction given the data records
        pass

    @abstractmethod
    def _db_transaction(self, kgdb: NoSQLKnowledgeGraph, option_name: str, data) -> None:
        # defines the db transaction that this benchmark run should compare
        pass


class NodeImportBenchmark(KGDBBenchmark):
    def __init__(self, benchmark_name: str,
                 option_1: NoSQLKnowledgeGraph,
                 option_2: NoSQLKnowledgeGraph,
                 import_lim: int,
                 option_1_name: str = "Firestore",
                 option_2_name: str = "Aura",
                 ):
        super().__init__(benchmark_name, option_1, option_2,
                         import_lim, option_1_name, option_2_name)

    def _construct_data(self, row: Any):
        # constructs NodeData given a tuple[str, str, str] record
        record_values = row.values()

        body_str = json.loads(record_values[1])[0]
        alias_list = json.loads(record_values[2])
        node_uid = record_values[0]

        node_data = NodeData(node_uid=node_uid,
                             node_title=node_uid,
                             node_description=body_str,
                             node_degree=0,
                             node_type="na",
                             document_id="na")
        return node_data

    def _db_transaction(self, kgdb: NoSQLKnowledgeGraph, option_name, data: NodeData) -> None:
        # defines the db transaction that this benchmark run should compare
        try:
            kgdb.add_node(node_uid=data.node_uid, node_data=data)
            print(f'Success adding node {data.node_uid} with {option_name}')
        except Exception as e:
            print(f"Error adding node {data.node_uid} with {option_name}: {e}")
            pass
        return None


class EdgeImportBenchmark(KGDBBenchmark):
    def __init__(self, benchmark_name: str,
                 option_1: NoSQLKnowledgeGraph,
                 option_2: NoSQLKnowledgeGraph,
                 import_lim: int,
                 option_1_name: str = "Firestore",
                 option_2_name: str = "Aura"
                 ):
        super().__init__(benchmark_name, option_1, option_2,
                         import_lim, option_1_name, option_2_name)

    def _construct_data(self, row: Any):
        # constructs NodeData given a tuple[str, str, str] record
        record_values = row.values()

        source_uid = row[0]
        edge_uid = row[1]
        target_uid = row[2]
        description_body = json.loads(row.values()[3])
        edge_description = json.loads(row.values()[3])[0]

        edge_data = EdgeData(source_uid=source_uid,
                             target_uid=target_uid,
                             description=edge_description,
                             edge_uid=edge_uid
                             )
        return edge_data

    def _db_transaction(self, kgdb: NoSQLKnowledgeGraph, option_name: str, data: EdgeData) -> None:
        # defines the db transaction that this benchmark run should compare
        try:
            kgdb.add_edge(edge_data=data, directed=True)
            print(f'Success adding edge {data.edge_uid} with {option_name}')
        except Exception as e:
            print(f"Error adding edge {data.edge_uid} with {option_name}: {e}")
            pass
        return None


class NodeQueryBenchmark(KGDBBenchmark): 
    def __init__(self, benchmark_name: str,
                 option_1: NoSQLKnowledgeGraph,
                 option_2: NoSQLKnowledgeGraph,
                 import_lim: int,
                 option_1_name: str = "Firestore",
                 option_2_name: str = "Aura"
                 ):
        super().__init__(benchmark_name, option_1, option_2,
                         import_lim, option_1_name, option_2_name)

    def _construct_data(self, row: Any):
        record_values = row.values()
        node_uid = record_values[0]
        return  node_uid

    def _db_transaction(self, kgdb: NoSQLKnowledgeGraph, option_name, data: str):
        # defines the db transaction that this benchmark run should compare
        try:
            kgdb.get_node(node_uid=data)
            print(f'Success fetching node data {data} with {option_name}')
        except Exception as e:
            print(f"Error fetching node data {data} with {option_name}: {e}")
            pass
        return None

        
if __name__ == "__main__":
    os.chdir('../')
    current_directory = os.getcwd()
    print(f"Current directory: {current_directory}")

    secrets = dotenv_values(".env")
    credentials, _ = google.auth.load_credentials_from_file(
        secrets["GCP_CREDENTIAL_FILE"])

    import_lim = 100

    # Fetch Node data from BigQuery
    client = bigquery.Client(project=str(
        secrets["GCP_PROJECT_ID"]), credentials=credentials)

    fskg = FirestoreKG(gcp_credential_file=str(secrets["GCP_CREDENTIAL_FILE"]),
                       gcp_project_id=str(secrets["GCP_PROJECT_ID"]),
                       firestore_db_id=str(secrets["FIRESTORE_DB_ID"]),
                       node_collection_id=str(secrets["NODE_COLL_ID"]),
                       edges_collection_id=str(secrets["EDGES_COLL_ID"]),
                       community_collection_id=str(
        secrets["COMM_COLL_ID"])
    )

    aura_kg = AuraKG(uri=str(secrets["NEO4J_URI"]),
                     auth=(str(secrets["NEO4J_USERNAME"]),
                           str(secrets["NEO4J_PASSWORD"]))
                     )

    # clean kg storages before starting test run
    # fskg.flush_kg()
    # aura_kg.flush_kg()

    # # # add nodes testing
    # query_job = client.query(
    #     f"SELECT * FROM poerschmann-sem-search.wikidata_kg.entity_doc_alias_joined LIMIT {import_lim}")
    # add_nodes_testing = NodeImportBenchmark(benchmark_name="Node Import", option_1=fskg, option_2=aura_kg, import_lim=100)
    # add_nodes_testing(records=query_job)

    # # add egdes testing
    # query_job = client.query(
    #     f"SELECT * FROM poerschmann-sem-search.wikidata_kg.triplets_relations_joined")
    # add_edges_testing = EdgeImportBenchmark(benchmark_name="Edge Import", option_1=fskg, option_2=aura_kg, import_lim=100)
    # add_edges_testing(records=query_job)

    print('hello base!')
