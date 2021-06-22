import json
import requests
import sys
import csv
import pandas as pd
import copy
import bisect
from typing import Any, List, TypedDict, Optional, cast

NODE_TABLE_NAME = 'nodes'
EDGE_TABLE_NAME = 'links'

# TODO: Change to something better
NETWORK_NAME = 'network'

def main():
    if len(sys.argv) < 3:
        print("usage: multinet.py <instance-url> <workspace>", file=sys.stderr)
        return 1

    # Extract args
    _, base_url, workspace = sys.argv

    # Delete network if it exists
    network_url = f'{base_url}/api/workspaces/{workspace}/graphs/{NETWORK_NAME}'
    r = requests.get(network_url)
    if r.status_code != 404:
        requests.delete(network_url)

    # Delete node table if it exists
    r = requests.get(f'{base_url}/api/workspaces/{workspace}/tables/{NODE_TABLE_NAME}')
    if r.status_code != 404:
        # TODO
        pass

    # Delete edge table if it exists
    r = requests.get(f'{base_url}/api/workspaces/{workspace}/tables/{EDGE_TABLE_NAME}')
    if r.status_code != 404:
        # TODO
        pass





if __name__ == "__main__":
    sys.exit(main())
