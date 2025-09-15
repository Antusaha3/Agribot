# test_graph_retriever.py
import os
os.environ.setdefault("NEO4J_URI","neo4j://127.0.0.1:7687")
os.environ.setdefault("NEO4J_USER","neo4j")
os.environ.setdefault("NEO4J_PASSWORD","12345agri")

from rag.retriever_graph import graph_answer_for_crop

print(graph_answer_for_crop("Aman"))     # English
print(graph_answer_for_crop("আমন"))      # Bangla
