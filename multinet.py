import shutil
import time
import tempfile
from typing import Dict, List, Set
import sys
import csv
from requests.models import Response
from requests.exceptions import HTTPError
from requests_toolbelt.sessions import BaseUrlSession
from s3_file_field_client import S3FileFieldClient

NODE_TABLE_NAME = "nodes"
EDGE_TABLE_NAME = "links"
ISSUES_TABLE_NAME = "issues"

# TODO: Change to something better
NETWORK_NAME = "network"


def raise_for_status(r: Response):
    """A wrapper for repsonse.raise_for_status."""
    try:
        r.raise_for_status()
    except HTTPError as error:
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


def await_tasks_finished(api_client: BaseUrlSession, tasks: List[Dict]):
    tasks_set: Set[int] = {t["id"] for t in tasks}
    sleep_time = 0.1
    while tasks_set:
        sleep_time *= 2
        time.sleep(sleep_time)
        for task_id in list(tasks_set):
            r = api_client.get(f"uploads/{task_id}/")
            raise_for_status(r)

            if r.json()["status"] == "FAILED":
                errors = r.json()["error_messages"]
                raise Exception(
                    f"Upload with Task ID {task_id} failed with errors: {errors}"
                )

            if r.json()["status"] == "FINISHED":
                tasks_set.remove(task_id)


def main():
    if len(sys.argv) < 3:
        print(
            "usage: multinet.py <instance-url> <workspace> <api-token>", file=sys.stderr
        )
        return 1

    # Extract args
    _, base_url, workspace, api_token = sys.argv

    # Inject auth token into every request
    api_client = BaseUrlSession(base_url=base_url)
    api_client.headers.update({"Authorization": f"Token {api_token}"})

    print("Uploading files...")

    # Upload all files to S3
    s3ff_client = S3FileFieldClient("/api/s3-upload/", api_client)

    # Upload nodes.csv
    with open("./nodes.csv", "rb") as file_stream:
        nodes_field_value = s3ff_client.upload_file(
            file_stream, "nodes.csv", "api.Upload.blob"
        )["field_value"]

    # Upload links.csv
    fix_links_csv()
    with open("./links.csv", "rb") as file_stream:
        links_field_value = s3ff_client.upload_file(
            file_stream, "links.csv", "api.Upload.blob"
        )["field_value"]

    # Upload issues.csv (not used in network)
    with open("./issues.csv", "rb") as file_stream:
        issues_field_value = s3ff_client.upload_file(
            file_stream, "issues.csv", "api.Upload.blob"
        )["field_value"]

    # Update base url, since only workspace endpoints are needed now
    api_client.base_url = f"{base_url}/api/workspaces/{workspace}/"

    # Delete network and tables if they exist
    api_client.delete(f"networks/{NETWORK_NAME}/")
    api_client.delete(f"tables/{NODE_TABLE_NAME}/")
    api_client.delete(f"tables/{EDGE_TABLE_NAME}/")
    api_client.delete(f"tables/{ISSUES_TABLE_NAME}/")

    # Create nodes table
    r = api_client.post(
        "uploads/csv/",
        json={
            "field_value": nodes_field_value,
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
    raise_for_status(r)
    nodes_upload = r.json()

    # Create links table
    r = api_client.post(
        "uploads/csv/",
        json={
            "field_value": links_field_value,
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
    raise_for_status(r)
    links_upload = r.json()

    # Create issues table
    r = api_client.post(
        "uploads/csv/",
        json={
            "field_value": issues_field_value,
            "edge": False,
            "table_name": ISSUES_TABLE_NAME,
            "columns": {},
        },
    )
    raise_for_status(r)
    issues_upload = r.json()

    print("Processing files...")

    # Wait for nodes and links tables to be created
    await_tasks_finished(api_client, [nodes_upload, links_upload])

    # Create network
    raise_for_status(
        api_client.post(
            "networks/",
            json={"name": NETWORK_NAME, "edge_table": EDGE_TABLE_NAME},
        )
    )

    print("Network created.")
    print("Processing issues (this may take some time)...")

    # Wait for issues to finish being processed
    await_tasks_finished(api_client, [issues_upload])

    print("Synchronization finished.")


if __name__ == "__main__":
    sys.exit(main())
