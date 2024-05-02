from neomodel import NodeSet
from .models import Nodes
from .logging import logger


def retrieve_nodes(node_label: str) -> NodeSet:
    """
    Retrieves all nodes from the Neo4j database that match a given node label.

    This function takes a string representing the node label, converts it to uppercase,
    and checks if it corresponds to any of the enum members in the Nodes class. If a match is found,
    it retrieves all nodes of that type using neomodel's NodeSet. If no match is found or an error occurs,
    it logs the error and returns an empty NodeSet.

    Args:
        node_label (str): The label of the node type to retrieve. This should match one of the labels
                          defined in the Nodes enum.

    Returns:
        NodeSet: A NodeSet of nodes of the specified type. Returns an empty NodeSet if the label is not found
                 or if an error occurs.

    Raises:
        Exception: Logs an error message with details of why the retrieval failed. If an error occurs,
                   an empty NodeSet is returned to maintain consistency in return type.
    """
    label = node_label.upper()
    try:
        # Accessing the node class using .value after checking if the label exists in the enum
        if label in Nodes.__members__:
            Node = Nodes[label].value
            nodes = Node.nodes.all()
            return nodes
        else:
            logger.error(f"Label '{label}' not found in Nodes enum.")
            return NodeSet()
    except Exception as e:
        # Improved error logging to capture which label caused the issue
        logger.error(f"Failed to retrieve nodes for label '{label}': {e}")
        return NodeSet()
