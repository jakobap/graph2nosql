# graph2nosql
A simple Python interface to store and interact with knowledge graphs in your favourite NoSQL DB.

Knowledge Graphs are the up and coming tool to index knowlegde and make it understandable to your LLM applications. Working with Graph Databases are a pain though.

This Python interface aims to solve  this by offering a set of basic functions to store and manage your (knowledge graph) in your existing NoSQL DB. `graph2nosql.py` is the abstract class defining the available operations.

This is mostly catering own use and currently not regularly updated or maintained.

### Implemented NoSQL Databases so far:
* Firestore

### Contributing
* If you decide to add new DB operations, please add corresponding tests to `graph2nosql_tests.py` 
* If you decide to write an implementation for another NoSQL db please make sure all tests in `graph2nosql_tests.py` succeed.