"""graph2nosql latency benchmark library across graph storage implementations"""

import os
import json
import time
from abc import ABC, abstractmethod
from typing import Any, Dict

from dotenv import dotenv_values

from google.cloud import bigquery
import google.auth 


from base.operations import NoSQLKnowledgeGraph
from databases.firestore_kg import FirestoreKG
from databases.n4j import AuraKG

from datamodel.data_model import NodeData, EdgeData


class KGDBBenchmarkDataFetcher:
    def __init__(self):
        pass


class BigQueryDataFetcher:
    def __init__(self):
        pass


class KGDBBenchmark(ABC):
    """
    Abstract base class for defining latency benchmark experiments for different Knowledge Graph Databases (KGDBs).

    This class provides a framework for comparing the performance of two different KGDB implementations
    on specific database operations. Concrete benchmark classes should inherit from this class and implement
    the `_construct_data` and `_db_transaction` methods.

    Attributes:
        benchmark_name (str): The name of the benchmark experiment.
        option_1 (NoSQLKnowledgeGraph): The first KGDB implementation being compared.
        option_2 (NoSQLKnowledgeGraph): The second KGDB implementation being compared.
        import_lim (int): The number of records to import/process in the benchmark.
        option_1_name (str, optional): A descriptive name for the first KGDB option. Defaults to "".
        option_2_name (str, optional): A descriptive name for the second KGDB option. Defaults to "".
        option_1_time (float): The total time taken by option_1 for the benchmark.
        option_2_time (float): The total time taken by option_2 for the benchmark.

    Example Usage:
        ```python
        class MyBenchmark(KGDBBenchmark):
            def __init__(self, option_1, option_2, import_lim):
                super().__init__("My Benchmark", option_1, option_2, import_lim, "Option A", "Option B")

            def _construct_data(self, row):
                # Implement logic to construct data for the benchmark from a row of input data.
                pass

            def _db_transaction(self, kgdb, option_name, data):
                # Implement the specific database operation to benchmark using the provided kgdb and data.
                pass

        # Create instances of your KGDB implementations (e.g., FirestoreKG, AuraKG)
        option_1 = ...
        option_2 = ...

        # Create an instance of your benchmark class
        benchmark = MyBenchmark(option_1, option_2, 1000)

        # Execute the benchmark
        benchmark(records)  # 'records' would be your input data
        ```
    """

    def __init__(self,
                 benchmark_name: str,
                 options_dict: Dict[str, NoSQLKnowledgeGraph],
                 import_lim: int,
                 ):
        self.benchmark_name = benchmark_name
        self.import_lim = import_lim
        self.options_dict = options_dict
        self.option_names = list(options_dict.keys())
        self.option_times = {}

    def __call__(self, records):

        print(
            f'$$$$ Starting Benchmark {self.benchmark_name} with options: {self.option_names} $$$$')

        for option_name in self.option_names:   
            start_time = time.time()

            for row in records:
                data = self._construct_data(row)
                try:
                    self._db_transaction(kgdb=self.options_dict[option_name],
                                        data=data, option_name=option_name)
                except Exception:
                    pass

            end_time = time.time()
            self.option_times[option_name] = end_time - start_time

        self._benchmark_reporting()
        print("hEllO wOrlD!")

    def _benchmark_reporting(self) -> None:
        for option_name in self.option_names:
            print(f'{option_name} time for {self.import_lim} {self.benchmark_name}: {self.option_times[option_name]}')
        return None

    @abstractmethod
    def _construct_data(self, row: Any):
        """constructs the data to be used for the benchmark db transaction given the data records"""

    @abstractmethod
    def _db_transaction(self, kgdb: NoSQLKnowledgeGraph, option_name: str, data) -> None:
        """defines the db transaction that this benchmark run should compare"""


class NodeImportBenchmark(KGDBBenchmark):
    """
    Define Latency Benchmark for Node Import. Inhertits from KGDBBenchmark.
    Implements _construct_data and _db_transaction methods for edge import.
    """
    def __init__(self,
                 benchmark_name: str,
                 options_dict: Dict[str, NoSQLKnowledgeGraph],
                 import_lim: int,
                 ):
        super().__init__(benchmark_name, options_dict, import_lim)

    def _construct_data(self, row: Any):
        # constructs NodeData given a tuple[str, str, str] record
        record_values = row.values()

        body_str = json.loads(record_values[1])[0]
        # alias_list = json.loads(record_values[2])
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
            # print(f'Success adding node {data.node_uid} with {option_name}')
        except Exception as e:
            print(f"Error adding node {data.node_uid} with {option_name}: {e}")


class EdgeImportBenchmark(KGDBBenchmark):
    """
    Define Latency Benchmark for edge import. Inhertits from KGDBBenchmark.
    Implements _construct_data and _db_transaction methods for edge import.
    """
    def __init__(self,
                 benchmark_name: str,
                 options_dict: Dict[str, NoSQLKnowledgeGraph],
                 import_lim: int,
                 ):
        super().__init__(benchmark_name, options_dict, import_lim)

    def _construct_data(self, row: Any):
        # constructs NodeData given a tuple[str, str, str] record
        # record_values = row.values()

        source_uid = row[0]
        edge_uid = row[1]
        target_uid = row[2]
        # description_body = json.loads(row.values()[3])
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
            # print(f'Success adding edge {data.edge_uid} with {option_name}')
        except Exception as e:
            print(f"Error adding edge {data.edge_uid} with {option_name}: {e}")
        return None


class NodeQueryBenchmark(KGDBBenchmark):
    """
    Define Latency Benchmark for node query. Inhertits from KGDBBenchmark.
    Implements _construct_data and _db_transaction methods for node query.
    """
    def __init__(self,
                 benchmark_name: str,
                 options_dict: Dict[str, NoSQLKnowledgeGraph],
                 import_lim: int,
                 ):
        super().__init__(benchmark_name, options_dict, import_lim)

    def _construct_data(self, row: Any):
        record_values = row.values()
        node_uid = record_values[0]
        return  node_uid

    def _db_transaction(self, kgdb: NoSQLKnowledgeGraph, option_name, data: str):
        # defines the db transaction that this benchmark run should compare
        try:
            kgdb.get_node(node_uid=data)
            # print(f'Success fetching node data {data} with {option_name}')
        except Exception as e:
            print(f"Error fetching node data {data} with {option_name}: {e}")
        return None

        
if __name__ == "__main__":
    os.chdir('../')
    current_directory = os.getcwd()
    print(f"Current directory: {current_directory}")

    secrets = dotenv_values(".env")
    credentials, _ = google.auth.load_credentials_from_file(
        secrets["GCP_CREDENTIAL_FILE"])

    IMPORT_LIMIT = 100

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
