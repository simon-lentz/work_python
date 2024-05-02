import logging
from .models import Nodes

logger = logging.getLogger("Neo4j ORM")


def retrieve_nodes(node_label: str) -> list:
    label = node_label.upper()
    try:
        # Accessing the node class using .value after checking if the label exists in the enum
        if label in Nodes.__members__:
            Node = Nodes[label].value
            nodes = Node.nodes.all()
            return nodes
        else:
            logger.error(f"Label '{label}' not found in Nodes enum.")
            return []
    except Exception as e:
        # Improved error logging to capture which label caused the issue
        logger.error(f"Failed to retrieve nodes for label '{label}': {e}")
        return []
