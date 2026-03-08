"""
Simple script to pull a file artifact from Multiplan's on-prem deployment of 
Domino data lab and save that file in a user specified location.

Developed using:
- python: 3.9.18
- dominodatalab (python package installed via pip): 1.2.4

Example usage:
python pull_domino_file.py --domino_project=<insert value> \
    --api_key=<insert value> \
    --project_commit_id=<insert value> \
    --target_file_path=<insert value> \
    --output_path=<insert value>
"""

## imports and set globals
from domino import Domino
import argparse


## helper function def - pull file from Domino
def pull_and_export_file(domino_project, api_key, project_commit_id,
                         target_file_path, output_path):
    """
    Helper function to establish a connection to the Domino API, pull a file 
    artifact, and then save it to user specified output location.

    Parameters
    ----------
    domino_project: str
        Project owner and project name of the Domino project that a file will be 
        pulled from. This should be passed as a string and formatted as:
        <project owner>/<project name>.

    api_key: str
        API key for the service account to be used in authenticating with Domino.

    project_commit_id: str
        Domino project commit id for the project snapshot that the file in 
        question will be pulled from.

    target_file_path: str
        Filename, with extension, of the file artifact to be pulled.

    output_path: str
        Absolute path, including filename and extension, where the downloaded 
        file should be written out to.

    Returns
    -------
    None - downloaded file is directly written out as part of the function call
    """
    # init Domino API wrapper class
    domino = Domino(
        project = domino_project,
        api_key = api_key,
        host = "https://domino.dsl.multiplan.com"
    )

    # get file download URL
    file = domino.files_list(
        commitId = project_commit_id,
        path = target_file_path
    )["data"]

    file_url = domino.blobs_get(file[0]["key"])

    # download and write file
    chunk_size = 16 * 1024

    with open(output_path, "wb") as f:
        while True:
            chunk = file_url.read(chunk_size)
            if not chunk:
                break
            f.write(chunk)




#### run it
if __name__ == "__main__":

    # argparsing
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--domino_project",
        type=str,
        required=True,
        help="Domino project owner and project name formatted as <project owner>/<project>"
    )

    parser.add_argument(
        "--api_key",
        type=str,
        required=True,
        help="Valid API key for authentication against the Domino API"
    )

    parser.add_argument(
        "--project_commit_id",
        type=str,
        required=True,
        help="Commit ID for project snapshot to pull file from"
    )

    parser.add_argument(
        "--target_file_path",
        type=str,
        required=True,
        help="Filename and extension, as defined in Domino, of the artifact to be downloaded"
    )

    parser.add_argument(
        "--output_path",
        type=str,
        required=True,
        help="Absolute path of the location where the downloaded file will be saved"
    )

    args = parser.parse_args()

    ## download file
    print("[+] Starting file download")

    pull_and_export_file(
        domino_project = args.domino_project,
        api_key = args.api_key,
        project_commit_id = args.project_commit_id,
        target_file_path = args.target_file_path,
        output_path = args.output_path
    )
    
    print("[+] File download and export complete")