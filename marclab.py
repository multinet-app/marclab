import json
import requests
import sys
import csv
import pandas as pd
import copy
import bisect
import os
from typing import Any, List, TypedDict, Optional, cast


class StructureTypes(TypedDict):
    ID: int
    ParentID: Optional[int]
    Name: str
    Notes: str
    MarkupType: str
    Tags: None
    StructureTags: str
    Abstract: bool
    Color: int
    Version: str
    Code: str
    HotKey: str
    Username: str
    LastModified: str
    Created: str

def base_url(network: str) -> str:
    if not network:
        raise RuntimeError("`network` must not be empty")

    return f"https://websvc1.connectomes.utah.edu/{network}/OData"

def network_url(network: str) -> str:
    if not network:
        raise RuntimeError("`network` must not be empty")

    return f"http://websvc1.connectomes.utah.edu/{network}/export/network/JSON"


def data_url(network: str, data_type: str) -> str:
    if not data_type:
        raise RuntimeError("`data_type` must not be empty")

    return f"{base_url(network)}/{data_type}"


def get_data(network: str, data_type: str) -> List[Any]:
    if data_type == "network":
        url = network_url(network)
        data = []
        data = requests.get(url).json()
    else:
        url = data_url(network, data_type)
        data = []
        page_num = 1

        print(f"Retrieving {data_type}...")
        while url:
            print(f"  page {page_num}...", file=sys.stderr, end="", flush=True)
            page = requests.get(url).json()
            print("done")

            data += page["value"]

            url = page.get("@odata.nextLink")
            page_num += 1

    return data


def main():
    # Grab the network name from the command line.
    if len(sys.argv) < 2:
        print("usage: marclab.py <network-name>", file=sys.stderr)
        return 1
    network_name = sys.argv[1]

    # Key for telling us the structure types in network
    structure_types = cast(List[StructureTypes], get_data(network_name, "StructureTypes"))
    with open("structure_type.json", "w") as f:
        f.write(json.dumps(structure_types))
    
    network = get_data(network_name, "network")
    with open("network.json", "w") as f:
        f.write(json.dumps(network))

    # Create dict of structure types (cells and synapses)
    type_id_dict = {}
    for structure in structure_types:
        type_id_dict[structure['ID']] = structure['Name'].strip()

    # Make the nice node csv
    better_node_keys = {'StructureID': 'StructureID', 'TypeID': 'TypeID', 'Label': 'Label', 'Volume': 'Volume (nm^3)',
                        'MaxDimension': 'MaxDimension', 'MinZ': 'MinZ', 'MaxZ': 'MaxZ'}

    nodes = []
    for node in network['nodes']:
        node_obj = {}
        for key, value in better_node_keys.items():
            node_obj[value] = node.get(key, '')
        node_obj['StructureType'] = type_id_dict[node['TypeID']]
        node_obj['_key'] = node['StructureID']
        nodes.append(node_obj)

    # Make nice edge csv
    better_edge_keys = {'ID': 'ID', 'SourceStructureID': 'SourceStructureID', 'TargetStructureID': 'TargetStructureID',
                        'Label': 'Label', 'Type': 'Type', 'Directional': 'Directional', 'Links': '#ofChildren'}
    edges = []
    for edge in network['edges']:
        edge_obj = {}
        for key, value in better_edge_keys.items():
            if key == 'Links':
                # Get # of children
                edge_obj[value] = len(edge.get(key, ''))
                # Store list of child objects
                edge_obj[key] = edge.get(key, '')
            elif key == 'SourceStructureID':
                edge_obj['_from'] = f"{network_name}_nodes/{str(edge.get(key, ''))}"
            elif key == 'TargetStructureID':
                edge_obj['_to'] = f"{network_name}_nodes/{str(edge.get(key, ''))}"
            else:
                edge_obj[value] = edge.get(key, '')
        edges.append(edge_obj)

    # Make artifacts folder
    os.makedirs('artifacts', exist_ok=True)

    # Create links file
    link_keys = edges[0].keys()
    with open('artifacts/links.csv', 'w', newline='') as f:
        dict_writer = csv.DictWriter(f, link_keys)
        dict_writer.writeheader()
        dict_writer.writerows(edges)

    # Create nodes file
    node_keys = nodes[0].keys()
    with open('artifacts/nodes.csv', 'w', newline='') as f:
        dict_writer = csv.DictWriter(f, node_keys)
        dict_writer.writeheader()
        dict_writer.writerows(nodes)

    return 0


if __name__ == "__main__":
    sys.exit(main())
