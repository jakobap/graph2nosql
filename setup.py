from setuptools import setup, find_packages

setup(
    name='graph2nosql',  # Choose a name for your library
    version='0.1', 
    packages=find_packages(),
    install_requires=[
        'networkx==3.3',
        'matplotlib==3.9.1',
        'langfuse==2.39.2',
        'graspologic==3.4.1',
        'numpy==2.0.1',
        'firebase-admin==6.5.0',
        'python-dotenv==1.0.0'
    ]
)