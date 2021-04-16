import json
import requests
import sys
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

    return 0


if __name__ == "__main__":
    sys.exit(main())
