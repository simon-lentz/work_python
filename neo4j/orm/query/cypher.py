import logging
from neomodel import db
from setup.models import State, County, City

logger = logging.getLogger("Neo4j ORM")


def fetch_state_node() -> State:
    # cypher query to return single state node
    get_state_node = '''
    MATCH (n:State)
    RETURN n as State
    LIMIT 1;
    '''
    # Inflate sample state node to python native object
    try:
        state_node, _ = db.cypher_query(query=get_state_node, params=None, resolve_objects=True)
        return state_node
    except Exception as e:
        logger.error(f"Failed to retrieve and parse sample State node: {e}")


def fetch_county_node() -> County:
    # cypher query to return single county node
    get_county_node = '''
    MATCH (n:County)
    RETURN n as County
    LIMIT 1;
    '''
    # Inflate sample county node to python native object
    try:
        county_node, _ = db.cypher_query(query=get_county_node, params=None, resolve_objects=True)
        return county_node
    except Exception as e:
        logger.error(f"Failed to retrieve and parse sample County node: {e}")


def fetch_city_node() -> City:
    # cypher query to return single city node
    get_city_node = '''
    MATCH (n:City)
    RETURN n as City
    LIMIT 1;
    '''
    # Inflate sample city node to python native object
    try:
        city_node, _ = db.cypher_query(query=get_city_node, params=None, resolve_objects=True)
        return city_node
    except Exception as e:
        logger.error(f"Failed to retrieve and parse sample City node: {e}")
