from neo4j import GraphDatabase
import pandas as pd

PORT = 7687
URI = f"bolt://localhost:{PORT}" 
password = "graph1234"
AUTH = ("neo4j", password)

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    print("Connection established.")

def fetchData():
    query = """
    MATCH (e:Employee)-[:CHARGES]->(t:Time)-[:IS_BILLED_FOR]->(p:Project)
    where p.source <> 'Unknown'
    WITH e, p, SUM(t.hours) AS total_time, avg(e.rate) AS average_rate
    RETURN p.project_type, p.source, e.employee_name, total_time, average_rate, total_time * average_rate AS total_cost
    ORDER BY total_cost DESC
    LIMIT 10
    """
    
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(query)
            data = [record.data() for record in result]
    
    return pd.DataFrame(data)

data = fetchData()
print(data.head()) 