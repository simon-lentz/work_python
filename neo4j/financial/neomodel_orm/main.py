import logging
from setup.config import make_db_connection
from query.cypher import (
    fetch_city_node, fetch_county_node, fetch_state_node
)
from query.apoc import (
    retrieve_city_nodes, retrieve_county_nodes, retrieve_state_nodes
)

logger = logging.Logger("neo4j financial")


def sample_cypher_queries():
    print(fetch_city_node())
    print(fetch_county_node())
    print(fetch_state_node())


def sample_apoc_queries():
    print(retrieve_city_nodes())
    print(retrieve_county_nodes())
    print(retrieve_state_nodes())


def main():
    if make_db_connection():
        sample_cypher_queries()
        sample_apoc_queries()
    else:
        logger.error("Unable to establish DB connection, exiting...")


if __name__ == '__main__':
    main()
