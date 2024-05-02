import logging
from pydantic import BaseModel
from neomodel import db, config


class Neo4jConfig(BaseModel):
    username: str = "neo4j"
    password: str = "alaskadb"
    address: str = "localhost"
    bolt_port: str = "7687"
    database_name: str = "neo4j"


def make_db_connection(logger: logging.Logger) -> bool:
    try:
        server = Neo4jConfig()
        config.DATABASE_URL = f"bolt://{server.username}:{server.password}@{server.address}:{server.bolt_port}"
        use_graph_query = f"USE {server.database_name}"
        connected, _ = db.cypher_query(f"{use_graph_query}\nRETURN 'Connection Successful' as message")
        logger.info(connected)
        return True
    except Exception as e:
        logger.error(f"Failed to connect to db: {e}")
        return None
