import json
import requests
import sys
from typing import Any, List


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
    structures = get_data("RPC1", "Structures")
    with open("structures.json", "w") as f:
        f.write(json.dumps(structures))

    structure_spatial_caches = get_data("RPC1", "StructureSpatialCaches")
    with open("structure_spatial_caches.json", "w") as f:
        f.write(json.dumps(structure_spatial_caches))

    structure_links = get_data("RPC1", "StructureLinks")
    with open("structure_links.json", "w") as f:
        f.write(json.dumps(structure_links))

    # TODO: cool stuff with this data, including turning it into Multinet
    # tables, etc.

    return 0


if __name__ == "__main__":
    sys.exit(main())
