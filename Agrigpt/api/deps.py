# api/deps.py
import os
from neo4j import GraphDatabase
import psycopg

# Neo4j driver
NEO4J_URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD", "12345agri")

def get_neo4j_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

# Postgres connection (pgvector)
def get_pg():
    return psycopg.connect(
        host=os.getenv("PGHOST","localhost"),
        port=os.getenv("PGPORT","5432"),
        dbname=os.getenv("PGDATABASE","agrigpt"),
        user=os.getenv("PGUSER","postgres"),
        password=os.getenv("PGPASSWORD","12345"),
    )
