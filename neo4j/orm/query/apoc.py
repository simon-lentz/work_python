import logging
from setup.models import State, County, City

logger = logging.getLogger("Neo4j ORM")


def retrieve_city_nodes() -> list:
    try:
        cities = City.nodes.all()
        return cities
    except Exception as e:
        logger.error(f"Failed to retrieve City nodes: {e}")
        return []


def retrieve_state_nodes() -> list:
    try:
        states = State.nodes.all()
        return states
    except Exception as e:
        logger.error(f"Failed to retrieve State nodes: {e}")
        return []


def retrieve_county_nodes() -> list:
    try:
        counties = County.nodes.all()
        return counties
    except Exception as e:
        logger.error(f"Failed to retrieve County nodes: {e}")
        return []
