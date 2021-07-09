import json
import requests
import sys
import csv
import pandas as pd
import copy
import bisect
from typing import Any, List, TypedDict, Optional, cast

NODE_TABLE_NAME = "nodes"
EDGE_TABLE_NAME = "links"

# TODO: Change to something better
NETWORK_NAME = "network"


def main():
    if len(sys.argv) < 3:
        print(
            "usage: multinet.py <instance-url> <workspace> <api-token>", file=sys.stderr
        )
        return 1

    # Extract args
    _, base_url, workspace, api_token = sys.argv

    # Inject auth token into every request
    api_client = requests.Session()
    api_client.headers.update({"Authorization": f"Token {api_token}"})

    # Delete network and tables if they exist
    workspace_url = f"{base_url}/api/workspaces/{workspace}"
    api_client.delete(f"{workspace_url}/networks/{NETWORK_NAME}")
    api_client.delete(f"{workspace_url}/tables/{NODE_TABLE_NAME}")
    api_client.delete(f"{workspace_url}/tables/{EDGE_TABLE_NAME}")


if __name__ == "__main__":
    sys.exit(main())
