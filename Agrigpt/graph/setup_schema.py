from pathlib import Path
from neo4j import GraphDatabase
import os, re

# Use bolt:// for a single local instance
URI  = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASSWORD", "12345agri")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")  # default db name unless you created a custom one

def strip_comments(cypher: str) -> str:
    cypher = re.sub(r"/\*.*?\*/", "", cypher, flags=re.S)  # /* ... */
    lines = []
    for line in cypher.splitlines():
        s = line.strip()
        if not s or s.startswith("//"):
            continue
        lines.append(line)
    return "\n".join(lines)

def iter_statements(cypher: str):
    for stmt in cypher.split(";"):
        s = stmt.strip()
        if s:
            yield s

def main():
    # <-- THIS is the correct file to read
    schema_file = Path(__file__).resolve().parent / "schema.cypher"
    print("Schema file:", schema_file)
    text = schema_file.read_text(encoding="utf-8")
    text = strip_comments(text)

    driver = GraphDatabase.driver(URI, auth=(USER, PASS))
    with driver.session(database=DB) as s:
        for stmt in iter_statements(text):
            print("Running:", stmt[:90].replace("\n"," "))
            s.run(stmt)
    driver.close()
    print("✓ Schema applied")

if __name__ == "__main__":
    main()
