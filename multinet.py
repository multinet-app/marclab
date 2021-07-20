import shutil
import time
import tempfile
import requests
import sys
import csv
from requests.models import Response
from s3_file_field_client import S3FileFieldClient

NODE_TABLE_NAME = "nodes"
EDGE_TABLE_NAME = "links"

# TODO: Change to something better
NETWORK_NAME = "network"


def raise_for_status(r: Response):
    """A wrapper for repsonse.raise_for_status."""
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as error:
        print(error.response.text)
        raise error


def fix_links_csv():
    outfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
    with open("links.csv", "r") as csvfile:
        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            if len(row["_from"].split("/")) != 2:
                row["_from"] = f"{NODE_TABLE_NAME}/{row['_from']}"
            if len(row["_to"].split("/")) != 2:
                row["_to"] = f"{NODE_TABLE_NAME}/{row['_to']}"
            writer.writerow(row)

    shutil.move(outfile.name, "links.csv")


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
    api_client.delete(f"{workspace_url}/networks/{NETWORK_NAME}/")
    api_client.delete(f"{workspace_url}/tables/{NODE_TABLE_NAME}/")
    api_client.delete(f"{workspace_url}/tables/{EDGE_TABLE_NAME}/")

    s3ff_client = S3FileFieldClient(f"{base_url}/api/s3-upload/", api_client)

    # Upload nodes.csv
    with open("./nodes.csv") as file_stream:
        field_value = s3ff_client.upload_file(
            file_stream, "nodes.csv", "api.Upload.blob"
        )["field_value"]

    raise_for_status(
        api_client.post(
            f"{workspace_url}/uploads/csv/",
            json={
                "field_value": field_value,
                "edge": False,
                "table_name": NODE_TABLE_NAME,
                "columns": {
                    "TypeID": "category",
                    "Verified": "boolean",
                    "Confidence": "number",
                    "ParentID": "category",
                    "Created": "date",
                    "LastModified": "date",
                    "TypeLabel": "category",
                    "Volume (nm^3)": "number",
                    "MaxDimension": "number",
                    "MinZ": "number",
                    "MaxZ": "number",
                },
            },
        )
    )

    # Upload links.csv
    fix_links_csv()
    with open("./links.csv") as file_stream:
        field_value = s3ff_client.upload_file(
            file_stream, "links.csv", "api.Upload.blob"
        )["field_value"]

    raise_for_status(
        api_client.post(
            f"{workspace_url}/uploads/csv/",
            json={
                "field_value": field_value,
                "edge": True,
                "table_name": EDGE_TABLE_NAME,
                "columns": {
                    "TotalChildren": "number",
                    "LastModified": "date",
                    "Bidirectional": "boolean",
                    "Type": "category",
                    "TotalSourceArea(nm^2)": "number",
                    "TotalTargetArea(nm^2)": "number",
                },
            },
        )
    )

    # Sleep to ensure files are processed
    time.sleep(1)

    # Create network
    raise_for_status(
        api_client.post(
            f"{workspace_url}/networks/",
            json={"name": NETWORK_NAME, "edge_table": EDGE_TABLE_NAME},
        )
    )


if __name__ == "__main__":
    sys.exit(main())
