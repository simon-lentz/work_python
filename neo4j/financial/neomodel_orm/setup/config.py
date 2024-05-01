import logging
from typing import Optional
from pydantic import BaseModel
from neomodel import config

logger = logging.getLogger(__name__)


class Neo4jConfig(BaseModel):
    username: str = "neo4j"
    password: str = "alaskadb"
    address: str = "localhost"
    bolt_port: str = "7687"
    database_name: Optional[str] = None


def make_db_connection() -> bool:
    try:
        db = Neo4jConfig()
        config.DATABASE_URL = f"bolt://{db.username}:{db.password}@{db.address}:{db.bolt_port}"
        return True
    except Exception as e:
        logger.error(f"Failed to connect to db: {e}")
        return False
