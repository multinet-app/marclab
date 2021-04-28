import json
import requests
import sys
import csv
from typing import Any, List, TypedDict, Optional, cast


class Structure(TypedDict):
    ID: int
    TypeID: int
    Notes: str
    Verified: bool
    Tags: str
    Confidence: float
    Version: str
    ParentID: Optional[int]
    Created: str
    Label: str
    Username: str
    LastModified: str


class Geometry(TypedDict):
    CoordinateSystemId: int
    WellKnownText: str
    WellKnownBinary: None


class GeometryDict(TypedDict):
    geometry: Geometry


class StructureSpatialCache(TypedDict):
    ID: int
    Area: float
    Volume: float
    MaxDimension: int
    MinZ: int
    MaxZ: int
    LastModified: str
    BoundingRect: GeometryDict
    ConvexHull: GeometryDict


class StructureLink(TypedDict):
    SourceID: int
    TargetID: int
    Bidirectional: bool
    Tags: None
    Username: str
    Created: str
    LastModified: str


def base_url(network: str) -> str:
    if not network:
        raise RuntimeError("`network` must not be empty")

    return f"https://websvc1.connectomes.utah.edu/{network}/OData"


def data_url(network: str, data_type: str) -> str:
    if not data_type:
        raise RuntimeError("`data_type` must not be empty")

    return f"{base_url(network)}/{data_type}"


def get_data(network: str, data_type: str) -> List[Any]:
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
    network = sys.argv[1]

    structures = cast(List[Structure], get_data(network, "Structures"))
    with open("structures.json", "w") as f:
        f.write(json.dumps(structures))

    structure_spatial_caches = cast(List[StructureSpatialCache], get_data(network, "StructureSpatialCaches"))
    with open("structure_spatial_caches.json", "w") as f:
        f.write(json.dumps(structure_spatial_caches))

    structure_links = cast(List[StructureLink], get_data(network, "StructureLinks"))
    with open("structure_links.json", "w") as f:
        f.write(json.dumps(structure_links))

    # TODO: cool stuff with this data, including turning it into Multinet
    # tables, etc.

    # function for making ID value a key
    def swap_key(dictionary, new_dict):
        for line in dictionary:
            for key, value in line.items():
                if (key == 'ID'):
                    if (new_dict.get(value)):
                        new_dict[value].append(line)
                    else:
                        new_dict[value] = []
                        new_dict[value].append(line)
        return new_dict

    # Create dictionary with ParentID values as keys
    structures_dict = {}
    structure_spatial_caches_dict = {}

    swap_key(structures, structures_dict)
    swap_key(structure_spatial_caches, structure_spatial_caches_dict)

    # Type dict for TypeID
    type_dict = {1: 'Cell', 3: 'Vessel', 28: 'Gap Junction', 31: 'Bipolar', 34: 'Conventional', 35: 'Postsynapse', 73: 'Ribbon Synapse', 80: 'Test', 81: 'Organized SER', 85: 'Adherens', 181: 'Cistern Pre', 182: 'Cistern Post', 183: 'Cilium', 189: 'BC Conventional Synapse', 219: 'Multi Plaque-like', 220: 'Endocytosis', 224: 'INL-IPL Boundary', 225: 'multivesicular body', 226: 'Ribosome patch', 227: 'Ribbon cluster', 229: 'Touch', 230: 'Loop', 232: 'Polysomes', 233: 'Depth', 234: 'Marker', 235: 'IPL-GCL Boundary', 236: 'Plaque', 237: 'axon', 240: 'Plaque-like Pre', 241: 'Plaque-like Post', 243: 'Neuroglial adherens', 244: 'Unknown', 245: 'Nucleolus', 246: 'Mitochondria', 247: 'Caveola', 248: 'Nuclear filament', 249: 'Golgi Plaque', 250: 'Golgi Normal', 252: 'Lysosome', 253: 'Annular Gap Junction', 254: 'Vessel Adjacency', 255: 'Rootlet', 256: 'CH Boundary', 257: 'Distal Junction', 258: 'Flat Contact', 259: 'Bubbles/Swirls', 260: 'Peri GJ Adherens', 261: 'Caveola String', 262: 'Coated Pits', 263: 'GJ Endo', 264: 'Dense Core Vesicle'}

    # Create links
    links = []
    i=1
    for line in structure_links:
        temp_dict = {}
        temp_dict['ID'] = i
        for key, value in line.items():
            if (key == 'SourceID'):
                temp_dict['SourceStructureID'] = structures_dict[value][0]['ParentID']
                temp_dict['TypeSource'] = type_dict[structures_dict[value][0]['TypeID']]
            if (key == 'TargetID'):
                temp_dict['TargetStructureID'] = structures_dict[value][0]['ParentID']
                temp_dict['TypeTarget'] = type_dict[structures_dict[value][0]['TypeID']]
            if (key == "Bidirectional"):
                temp_dict[key] = value
            if (key == "LastModified"):
                temp_dict[key] = value
        # Something is not working here
        # temp_dict["SourceArea"] = structure_spatial_caches_dict[temp_dict['SourceStructureID']][0]["Area"]
        # temp_dict["SourceMinZ"] = structure_spatial_caches_dict[temp_dict['SourceStructureID']][0]["MinZ"]
        # temp_dict["TargetArea"] = structure_spatial_caches_dict[temp_dict['TargetStructureID']][0]["Area"]
        # temp_dict["TargetMinZ"] = structure_spatial_caches_dict[temp_dict['TargetStructureID']][0]["MinZ"]
        # temp_dict["Label"] = temp_dict["SourceStructureID"]+"-"+temp_dict['TargetStructureID']+" via " + temp_dict['TypeSource']+" from " + line['SourceID'] + " -> " + line['TargetID']
        temp_dict["Links"] = [{
            "SourceID": line['SourceID'],
            "TargetID": line['TargetID'],
            "Directional": line['Bidirectional']
        }]
        links.append(temp_dict)
        i += 1

    # Create links file
    link_keys = links[0].keys()
    with open('links.csv', 'w', newline='') as f:
        dict_writer = csv.DictWriter(f, link_keys)
        dict_writer.writeheader()
        dict_writer.writerows(links)


    # Create nodes
    nodes = []
    for line in structures:
        temp_dict = {}
        for key,value in line.items():
            if (key != 'Notes' or 'Tags' or 'Version' or 'ParentID' or 'Username'):
                temp_dict[key] = value
        nodes.append(temp_dict)

    # Create nodes file
    node_keys = nodes[0].keys()
    with open('nodes.csv', 'w', newline='') as f:
        dict_writer = csv.DictWriter(f, node_keys)
        dict_writer.writeheader()
        dict_writer.writerows(nodes)

    return 0


if __name__ == "__main__":
    sys.exit(main())
