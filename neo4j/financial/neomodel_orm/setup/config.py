import logging
from typing import Optional
from pydantic import BaseModel
from neomodel import db, config

logger = logging.getLogger(__name__)


class Neo4jConfig(BaseModel):
    username: str = "neo4j"
    password: str = "alaskadb"
    address: str = "localhost"
    bolt_port: str = "7687"
    database_name: Optional[str] = None


def make_db_connection() -> bool:
    try:
        server = Neo4jConfig()
        config.DATABASE_URL = f"bolt://{server.username}:{server.password}@{server.address}:{server.bolt_port}"
        if server.database_name:
            config.DATABASE_NAME = server.database_name
        results, _ = db.cypher_query("RETURN 'Connection Successful' as message")
        logger.info(results)
        return True
    except Exception as e:
        logger.error(f"Failed to connect to db: {e}")
        return None
