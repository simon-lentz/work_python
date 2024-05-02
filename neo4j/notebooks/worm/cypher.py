from neomodel import db
from .logging import logger


def run_query(cypher_query: str) -> tuple:
    """
    Executes a Cypher query on the Neo4j database and returns the results along with metadata.

    This function takes a Cypher query string, executes it against the Neo4j database using neomodel's
    db.cypher_query method, and attempts to resolve returned objects based on the graph models defined.
    It returns both the results of the query and metadata about the results, such as column names.

    Args:
        cypher_query (str): The Cypher query string to be executed on the Neo4j database.

    Returns:
        tuple: A tuple containing two elements; the first is the list of results from the query, and the second
               is metadata about those results. If an exception occurs, it logs the error and the function
               returns None.

    Raises:
        Exception: Logs an error message detailing why the query failed. If an error occurs, None is returned,
                   highlighting an unsuccessful query execution.
    """
    try:
        results, meta = db.cypher_query(query=cypher_query, params=None, resolve_objects=True)
        return (results, meta)
    except Exception as e:
        logger.error(f"Failed to execute query '{cypher_query}': {e}")
        return None
