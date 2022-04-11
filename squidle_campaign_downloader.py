import json
import requests
import pandas as pd
import math
from multiprocessing import Pool
from tqdm import tqdm
import argparse
from pathlib import Path
from urllib.parse import urlparse


def imap_unordered_bar(func, args, n_processes=8):
    p = Pool(n_processes)
    res_list = []
    with tqdm(total=len(args)) as pbar:
        for _, res in tqdm(enumerate(p.imap_unordered(func, args))):
            pbar.update()
            res_list.append(res)
    pbar.close()
    p.close()
    p.join()
    return res_list


def find_deployments(session, campaign_list, url):
    """Finds the deployment IDs for a list of campaigns

    Parameters
    ----------
    session : requests.Session
        requests session object
    campaign_list : list
        List of campaign names
    url : str
        URL of SQUIDLE server

    Returns
    -------
    list
        List of deployment IDs
    """

    # Find the deployments for each campaign
    deployment_list = []
    for campaign_name in tqdm(campaign_list):
        print("Looking for campaign:", campaign_name)
        json_request = {
            "filters": [
                {
                    "name": "campaign",
                    "op": "has",
                    "val": {"name": "name", "op": "eq", "val": campaign_name},
                }
            ]
        }

        params = {"q": json.dumps(json_request), "single": True}

        r = session.get(
            url + "/api/deployment", params=params
        )

        if "<h1>500</h1>" in r.text:
            print("\nServer error\n")
        else:
            pretty_json = json.loads(r.text)

            for obj in pretty_json.get("objects"):
                deployment_list.append(obj.get("id"))

            print(" - Found deployment IDs:", deployment_list)

    return deployment_list


def find_images_in_deployments(session, deployment_list, url):
    """Finds images in a list of deployments

    Parameters
    ----------
    session : requests.Session
        requests session object
    deployment_list : list
        List of deployment IDs
    url : str
        URL of SQUIDLE server

    Returns
    -------
    list
        list of image IDs
    """
    # Find the image URL and image ID for each deployment
    image_ids = []
    image_deployment = []
    for deployment in tqdm(deployment_list):
        print("Looking for images in deployment:", deployment)
        json_request = {
            "filters": [
                {"name": "deployment_id", "op": "eq", "val": deployment}
            ]
        }

        results_per_page = 100

        params = {
            "q": json.dumps(json_request),
            "page": 1,
            "results_per_page": results_per_page,
            "single": True,
        }

        r = session.get(url + "/api/media", params=params)

        pretty_json = json.loads(r.text)

        num_results = pretty_json.get("num_results")

        for obj in pretty_json.get("objects"):
            image_ids.append(obj.get("id"))
            image_deployment.append(deployment)

        num_pages = math.ceil(num_results / results_per_page)


        for page in tqdm(range(2, num_pages)):
            params = {
                "q": json.dumps(json_request),
                "page": page,
                "results_per_page": results_per_page,
                "single": True,
            }

            r = session.get(url + "/api/media", params=params)

            pretty_json = json.loads(r.text)

            num_results = pretty_json.get("num_results")
            for obj in pretty_json.get("objects"):
                image_ids.append(obj.get("id"))
                image_deployment.append(deployment)
    return image_ids, image_deployment


def get_info_to_database(zipped_image_id_image_deployment):
    image_id, image_deployment = zipped_image_id_image_deployment
    r = requests.get(
        url + "/api/media_poses/" + str(image_id)
    )

    pretty_json = json.loads(r.text)

    media = pretty_json.get("media")
    pose = pretty_json.get("pose")

    vals = pd.DataFrame(
        {
            "timestamp": [pose.get("timestamp")],
            "lat": [float(pose.get("lat"))],
            "lon": [float(pose.get("lon"))],
            "dep": [float(pose.get("dep"))],
            "alt": [float(pose.get("alt"))],
            "image_url": [media.get("path_best")],
            "image_id": [int(image_id)],
            "deployment_id": [int(image_deployment)],
        }
    )

    return vals


def get_image_pose_and_url(
    image_ids, image_deployment
):
    database = pd.DataFrame(
        {
            "timestamp": [],
            "lat": [],
            "lon": [],
            "dep": [],
            "alt": [],
            "image_url": [],
            "image_id": [],
            "deployment_id": [],
        }
    )

    zipped_image_ids_deployment = [
        (a, b) for a, b in zip(image_ids, image_deployment)
    ]

    results = imap_unordered_bar(
        get_info_to_database, zipped_image_ids_deployment
    )

    for result in results:
        database = pd.concat([database, result], ignore_index=True)

    database.deployment_id = database.deployment_id.astype(int)
    database.image_id = database.image_id.astype(int)

    return database


def download_image_url(zipped_url_deployment_image_id):
    url, deployment_id, image_id, output_folder = zipped_url_deployment_image_id
    a = urlparse(url.rstrip())

    deployment_id = int(deployment_id)
    image_id = int(image_id)

    deployment_path = Path(output_folder) / str(deployment_id)
    if not deployment_path.exists():
        deployment_path.mkdir(parents=True, exist_ok=True)

    filename = deployment_path / (str(image_id) + ".png")

    res = requests.get(url.rstrip())
    img_data = res.content
    with open(filename, "wb") as handler:
        handler.write(img_data)
    return filename


if __name__ == "__main__":

    arg_parse = argparse.ArgumentParser()
    arg_parse.add_argument("--api-token", type=str, required=False, help="Provide your user API token to access private data. Not needed for public datasets.")
    arg_parse.add_argument("--campaign", type=str, required=True, nargs='+', help="List of campaign names (e.g. dy108-109_nerc_oceanids_class)")
    arg_parse.add_argument("--url", type=str, required=False,  default='https://soi.squidle.org', help="URL of squidle instance")
    arg_parse.add_argument("--output", type=str, required=False, default='squidle_download', help="Output folder")

    args = arg_parse.parse_args()

    url = args.url
    api_token = args.api_token
    campaign_list = args.campaign
    output = args.output

    if api_token is None:
        api_token = ""

    s = requests.Session()
    s.headers.update({"content-type": "application/json", "X-auth-token": api_token})

    deployment_list = find_deployments(s, campaign_list, url)
    image_list, image_deployment_list = find_images_in_deployments(
        s, deployment_list, url
    )
    print("Getting image poses and URLs (not downloading data yet)")
    database = get_image_pose_and_url(
        image_list, image_deployment_list
    )
    output_folder = Path(output)
    if not output_folder.exists():
        output_folder.mkdir(parents=True, exist_ok=True)

    print("Storing dataset CSV containing image URLs")
    database.to_csv(output_folder / "filelist.csv")
    print("Downloading images...")
    image_url_list = list(database["image_url"])
    image_id_list = list(database["image_id"])
    deployment_id_list = list(database["deployment_id"])

    zipped_list = [
        (a, b, c, output)
        for a, b, c in zip(image_url_list, deployment_id_list, image_id_list)
    ]

    _ = imap_unordered_bar(download_image_url, zipped_list)
    print("Done")
