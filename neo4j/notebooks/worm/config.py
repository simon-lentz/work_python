from pydantic import BaseModel
from neomodel import db, config
from .logging import logger


class Neo4jConfig(BaseModel):
    """
    Configuration class for Neo4j database connections.

    Attributes:
        username (str): Username for the Neo4j database.
        password (str): Password for the Neo4j database.
        address (str): Host address where the Neo4j database is running.
        bolt_port (str): Port number for the BOLT protocol.
        database_name (str): Name of the database to connect to.
    """
    username: str
    password: str
    address: str
    bolt_port: str
    database_name: str

    def __init__(self, username='neo4j', password='neo4j', address='localhost', bolt_port='7687', database_name='neo4j'):  # noqa:E501
        super().__init__(username=username, password=password, address=address, bolt_port=bolt_port, database_name=database_name)  # noqa:E501


def make_db_connection(neo4j_config: Neo4jConfig) -> bool:
    """
    Establish a connection to the Neo4j database using the provided configuration.

    This function constructs the database URL from the provided config, attempts to establish
    a connection, and logs the outcome.

    Args:
        neo4j_config (Neo4jConfig): The configuration object containing connection details.

    Returns:
        bool: True if the connection is successful, False otherwise.

    Raises:
        Exception: If unable to connect to the database, an exception is logged and None is returned.
    """
    try:
        config.DATABASE_URL = f"bolt://{neo4j_config.username}:{neo4j_config.password}@{neo4j_config.address}:{neo4j_config.bolt_port}"  # noqa:E501
        use_graph_query = f"USE {neo4j_config.database_name}"
        connected, _ = db.cypher_query(f"{use_graph_query}\nRETURN 'Connection Successful' as message")
        logger.info(connected)
        return True
    except Exception as e:
        logger.error(f"Failed to connect to db: {e}")
        return None
