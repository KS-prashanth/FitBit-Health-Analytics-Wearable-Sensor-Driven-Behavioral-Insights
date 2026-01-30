import logging
from neo4j import GraphDatabase

# Configure Neo4j credentials (ensure these are stored securely, e.g., in Lambda environment variables)
NEO4J_URI = "neo4j+s://9d24ccb0.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "ll0grkN_ME_vUSDlN2JMRd12xZ8m8Qmj25M0e5LZFuA"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        with driver.session() as session:
            # Step 1: Create Cluster Nodes if they do not exist
            create_clusters_query = """
            MERGE (a:ClusterA {name: "Cluster A"})
            MERGE (b:ClusterB {name: "Cluster B"})
            MERGE (c:ClusterC {name: "Cluster C"})
            MERGE (d:ClusterD {name: "Cluster D"})
            """
            session.run(create_clusters_query)
            logger.info("Cluster nodes ensured.")

            # Step 2: (Optional) Remove existing BELONGS_TO relationships if you want to reassign clusters
            # Uncomment the next query if you want to clear previous assignments
            clear_assignments_query = """
            MATCH (u:User)-[r:BELONGS_TO]->(c)
            DELETE r
            """
            session.run(clear_assignments_query)
            logger.info("Cleared existing cluster assignments.")

            # Step 3: Assign users to clusters based on age criteria.
            # We use FOREACH with CASE to conditionally MERGE relationships.
            assign_clusters_query = """
            MATCH (u:User)
            WHERE u.age IS NOT NULL
            FOREACH (_ IN CASE WHEN toInteger(u.age) >= 30 AND toInteger(u.age) < 35 THEN [1] ELSE [] END |
                MERGE (u)-[:BELONGS_TO]->(a:ClusterA {name: "Cluster A"})
            )
            FOREACH (_ IN CASE WHEN toInteger(u.age) >= 35 AND toInteger(u.age) <= 45 THEN [1] ELSE [] END |
                MERGE (u)-[:BELONGS_TO]->(b:ClusterB {name: "Cluster B"})
            )
            FOREACH (_ IN CASE WHEN toInteger(u.age) >= 46 AND toInteger(u.age) <= 55 THEN [1] ELSE [] END |
                MERGE (u)-[:BELONGS_TO]->(c:ClusterC {name: "Cluster C"})
            )
            FOREACH (_ IN CASE WHEN toInteger(u.age) >= 50 THEN [1] ELSE [] END |
                MERGE (u)-[:BELONGS_TO]->(d:ClusterD {name: "Cluster D"})
            )
            """
            session.run(assign_clusters_query)
            logger.info("Users assigned to clusters based on age.")

    except Exception as e:
        logger.error("Error updating clusters: " + str(e))
        raise
    finally:
        driver.close()

    return {"status": "Clusters updated successfully."}
