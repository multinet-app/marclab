import json
import requests
import sys
import csv
import pandas as pd
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
    
    structure_types = cast(List[StructureTypes], get_data(network, "StructureTypes"))
    with open("structure_type.json", "w") as f:
        f.write(json.dumps(structure_types))

    # TODO: cool stuff with this data, including turning it into Multinet
    # tables, etc.
    # List for tracking potential data issues
    issues = []

    # Create dict of structure types (cells and synapses)
    type_id_dict = {}
    for structure in structure_types:
        type_id_dict[structure['ID']] = structure['Name'].strip()


    # Combine structure information across structures and structure_spatial_caches
    # Separate into nodes (typeID = 1) and edges (typeID != 1)

    # function for making ID value a key
    def swap_key(dictionary, new_dict):
        for line in dictionary:
            # Add the Type Label
            line['TypeLabel'] = type_id_dict[line['TypeID']]
            # Add blank keys from Structural Cache
            # This will account for dif in sizes
            for key in structure_spatial_caches[0].keys():
                if key == 'Area':
                    line['Area (nm^2)'] = line.get(key, 'undefined')
                elif key == 'Volume':
                    line['Volume (nm^3)'] = line.get(key, 'undefined')
                else:
                    line[key] = line.get(key, 'undefined')
            new_dict[line['ID']] = line
        return new_dict


    # Create dictionary with ID values as keys
    structures_dict = {}
    swap_key(structures, structures_dict)

    # Add structure spatial cache to structures dict
    for structure in structure_spatial_caches:
        structure_copy = structure
        structure_copy['Area (nm^2)'] = structure_copy['Area']
        del structure_copy['Area']
        structure_copy['Volume (nm^3)'] = structure_copy['Volume']
        del structure_copy['Volume']
        structures_dict[structure_copy['ID']] = {**structures_dict[structure_copy['ID']], **structure_copy}

    # Separate the nodes (cells, Type ID == 1) from the edges (synapses, , Type ID != 1)
    # Remove attributes that don't make sense for structure type
    nodes = []
    edges_dict = {}

    for value in structures_dict.values():
        if value['TypeID'] == 1:
            value_copy = value
            del value_copy['Area (nm^2)']
            nodes.append(value_copy)
        else:
            if value['ParentID'] != 1:
                # Catch if children not assigned parent
                if not value['ParentID']:
                    noParentIssue = {'Type': 'Parentless child', 'Info': value}
                    issues.append(noParentIssue)
                # Catch if children are assigned another child as a parent
                else:
                    childAsParentIssues = {'Type': 'Child as parent', 'Info': value}
                    issues.append(childAsParentIssues)
            # Create edges dictionary with ID as key
            value_copy = value
            del value_copy['Volume (nm^3)']
            edges_dict[value_copy['ID']] = value_copy

    edges_parent_list = []
    # Link edges based on ParentID associated with the SourceID and TargetID
    for links in structure_links:
        if edges_dict.get(links['SourceID']) and edges_dict.get(links['TargetID']):
            links['_from'] = edges_dict[links['SourceID']]['ParentID']
            links['_to'] = edges_dict[links['TargetID']]['ParentID']
            for source_key, source_value in edges_dict[links['SourceID']].items():
                links['_from' + source_key] = source_value
            for target_key, target_value in edges_dict[links['TargetID']].items():
                links['_to' + target_key] = target_value
            edges_parent_list.append(links)

    # Group by source and target
    df = pd.DataFrame(edges_parent_list)
    links_df = df.groupby(['_to', '_from', '_fromTypeLabel', '_toTypeLabel']).groups

    links = []
    for row_index in links_df.values():
        # Construct label: _to-_from via '_fromTypeLabel' from 'SourceID' -> 'TargetID'
        # Keep track of number of children
        path = {'Label': '', 'TotalChildren': 0}
        for i in row_index:
            # Determine label for for synapse
            edgeType = ''
            if 'Pre' in df.loc[i, '_fromTypeLabel']:
                edgeType = df.loc[i, '_fromTypeLabel'].replace('Pre', '')
            elif 'Post' in df.loc[i, '_fromTypeLabel']:
                # Catch if a source label includes "Post"
                labelIssue = {'Type': 'Pre/Post Label',
                            'Info': 'SourceID: {}, labeled: {}'.format(df.loc[i, 'SourceID'],
                                                                            df.loc[i, '_fromTypeLabel'])}
                issues.append(labelIssue)
                edgeType = df.loc[i, '_fromTypeLabel']
            else:
                edgeType = df.loc[i, '_fromTypeLabel']
            path = {**path, **df.loc[i, ['_from', '_to', 'LastModified', 'Bidirectional']], 'Type': edgeType}
            if path['Label'] == '':
                # Account for bidirectional
                if path['Bidirectional']:
                    path['Label'] = '{}-{} via {} from {} <-> {}'.format(path['_from'], path['_to'], path['Type'],
                                                                        df.loc[i, 'SourceID'], df.loc[i, 'TargetID'])
                else:
                    path['Label'] = '{}-{} via {} from {} -> {}'.format(path['_from'], path['_to'], path['Type'],
                                                                        df.loc[i, 'SourceID'], df.loc[i, 'TargetID'])
            else:
                if path['Bidirectional']:
                    path['Label'] += ', {} <-> {}'.format(df.loc[i, 'SourceID'], df.loc[i, 'TargetID'])
                else:
                    path['Label'] += ', {} -> {}'.format(df.loc[i, 'SourceID'], df.loc[i, 'TargetID'])
            path['TotalChildren'] += 1
            # Add source and target area to path
            if df.loc[i, '_fromArea (nm^2)'] != 'undefined':
                path['TotalSourceArea(nm^2)'] = float(df.loc[i, '_fromArea (nm^2)'])
            else:
                # Catch if there is no area
                areaIssue = {'Type': 'Area undefined', 'Info': path}
                path['TotalSourceArea(nm^2)'] = 0
                issues.append(areaIssue)
            if df.loc[i, '_toArea (nm^2)'] != 'undefined':
                path['TotalTargetArea(nm^2)'] = float(df.loc[i, '_toArea (nm^2)'])
            else:
                # Catch if there is no area
                areaIssue = {'Type': 'Area undefined', 'Info': path}
                path['TotalTargetArea(nm^2)'] = 0
                issues.append(areaIssue)
            if path['Bidirectional'] and (round(path['TotalSourceArea(nm^2)']) != round(path['TotalTargetArea(nm^2)'])):
                # Catch if areas are not similar
                areaIssue = {'Type': 'Areas not similar',
                            'Info': path}
                issues.append(areaIssue)
        links.append(path)

    # Create links file
    link_keys = links[0].keys()
    with open('links.csv', 'w', newline='') as f:
        dict_writer = csv.DictWriter(f, link_keys)
        dict_writer.writeheader()
        dict_writer.writerows(links)

    # Create nodes file
    node_keys = nodes[0].keys()
    with open('nodes.csv', 'w', newline='') as f:
        dict_writer = csv.DictWriter(f, node_keys)
        dict_writer.writeheader()
        dict_writer.writerows(nodes)

    # Create issues files
    issue_keys = issues[0].keys()
    with open('issues.csv', 'w', newline='') as f:
        dict_writer = csv.DictWriter(f, issue_keys)
        dict_writer.writeheader()
        dict_writer.writerows(issues)

    return 0


if __name__ == "__main__":
    sys.exit(main())
