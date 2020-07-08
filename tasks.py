from invoke import task
import requests
import dateutil
import logging
from pathlib import Path
import datetime
from typing import Tuple
import unidecode
import collections
import arrow
import zipfile

logging.basicConfig(level=logging.INFO)

WORKING_DIR = Path("./tmp")


def _get_all_datasets():
    r = requests.get("http://localhost:5000/api/datasets")
    return r.json()


def _get_all_ressources():
    r = requests.get("https://transport.data.gouv.fr/api/datasets")
    datasets = r.json()

    for d in datasets:
        dataset_name = d["title"]
        for r in d["resources"]:
            url = r.get("url")
            if not url:
                continue

            yield {"dataset": d, "metadata": r}


def _debug_name(resource) -> str:
    return f'{resource["dataset"]["title"]} {resource["metadata"]["title"]} ({resource["metadata"]["url"]}) / {resource["dataset"]["id"]}'


def _resource_title(resource) -> str:
    return (
        f'{resource["metadata"]["title"]}'.replace(" ", "_")
        .replace("/", "_")
        .replace("'", "_")
    )


def _download_resource(resource) -> Tuple[Path, str]:
    # we create the working dir
    WORKING_DIR.mkdir(parents=True, exist_ok=True)

    resource_response = requests.get(resource["metadata"]["url"], allow_redirects=True)
    now = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    file_name = f"{_resource_title(resource)}_{now}"
    file_path = path = WORKING_DIR / file_name
    open(file_path, "wb").write(resource_response.content)
    return (file_path, file_name)


@task(default=True)
def check_fares(ctx):
    """
    check if there are some fares in the GTFS
    """
    for r in _get_all_ressources():
        # To speed up after first run
        if r["dataset"]["id"] not in [
            "5af03701b595081c1880a8a4",
            "5c9399f88b4c412a8b9315a8",
            "5c58758e8b4c415736c013a0",
            "5b4f299cc751df452b86361b",
            "5b4f299cc751df452b86361b",
            "5d1081696f444106b5aae5c7",
            "5c07a0588b4c41679e1c31f4",
            "5b873d7206e3e76e5b2ffd32",
            "5b873d7206e3e76e5b2ffd32",
            "588a238d88ee3846659b81a4",
            "5c07d931634f41746c7027d5",
            "5bec4c588b4c4165a5e3d43d",
            "5cebffba8b4c416359b36ef0",
            "5d4d41ba634f416d75efd449",
            "5d23ab778b4c416e54538a06",
            "5bc59247634f417c808580b3",
            "580a574aa3a7292dcfa9d1da",
            "580a574aa3a7292dcfa9d1da",
        ]:
            continue
        if r["metadata"]["format"] != "GTFS":
            continue
        path, fname = _download_resource(r)

        try:
            with zipfile.ZipFile(path, "r") as zipf:
                # print(f"files = {zipf.filelist}")
                for f in zipf.filelist:

                    if "fare" in f.filename:
                        print(f" ***********  {f.filename} dans {_debug_name(r)}")
                        data = zipf.read(f)

                        nb_line = len(data.decode("utf-8").split("\n"))
                        print(f"{nb_line} ==> {data}")

        except zipfile.BadZipFile as e:
            # print(f"error: {e} for {_debug_name(r)}")
            pass

        path.unlink()


@task()
def print_datasets(ctx):
    import csv

    with open("datasets.csv", "w", newline="") as csvfile:
        fieldnames = [
            "id",
            "type",
            "url",
            "titre",
            "couverture geographique",
            "type couverture geo",
            "réseaux",
        ]
        writer = csv.DictWriter(csvfile)

        writer.writerow(fieldnames)

        for d in _get_all_datasets():
            lieu = d["covered_area"]["name"]
            type_couv = d["covered_area"]["type"]
            networks = []
            for r in d.get("resources", []):
                networks.extend(r["networks"] or [])
            l = [
                d["id"],
                d["type"],
                f"https://transport.data.gouv.fr/datasets/{d['id']}",
                d["title"],
                lieu,
                type_couv,
            ]
            l.extend(set(networks))
            writer.writerow(l)


@task()
def use_stats(ctx):
    import csv

    resources = [r for r in _get_all_ressources()]
    # transport_resources_by_dataset_and_title = {(r["dataset"]["datagouv_id"], r["metadata"]["title"]): r["metadata"] for r in resources}
    transport_datasets = {d["datagouv_id"]: d for d in _get_all_datasets()}

    nb_not_found = 0
    added_dataset = set()

    with open("res_2019.csv", "w") as output_file:

        # with open("./test_limit_use.csv", "r") as stat_transport_file:
        with open("./stat_transport_2019.csv", "r") as stat_transport_file:
            stat_transport = csv.DictReader(stat_transport_file)
            fieldnames = stat_transport.fieldnames + [
                "dataset.id",
                "dataset.title",
                "dataset.slug",
                "dataset.url",
                "dataset.organization",
                "dataset.type",
                # "resource.title",
                # "resource.format"
            ]
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)

            writer.writeheader()

            use_by_url = {s["Méta-données: url"]: s for s in stat_transport}

            with open(
                # "./test.csv", "r"
                "./datagouv_resources_20200613.csv",
                "r",
            ) as datagouv_resources_file:
                datagouv_resources = csv.DictReader(
                    datagouv_resources_file, delimiter=";"
                )

                for dr in datagouv_resources:
                    dataset_id = dr["dataset.id"]
                    if dataset_id in added_dataset:
                        continue
                    url = dr["url"]

                    tr = use_by_url.get(url)
                    if tr is not None:
                        added_dataset.add(dataset_id)
                        # print(f"found a transport for {url}")

                        transport_dset = transport_datasets.get(dr["dataset.id"])
                        if transport_dset is not None:
                            new_line = {
                                "dataset.id": dr["dataset.id"],
                                "dataset.title": dr["dataset.title"],
                                "dataset.slug": dr["dataset.slug"],
                                "dataset.url": dr["dataset.url"],
                                "dataset.organization": dr["dataset.organization"],
                                "dataset.type": transport_dset["type"],
                                # "resource.title": dr["title"],
                                **tr,
                            }
                            writer.writerow(new_line)
                        else:
                            nb_not_found += 1
                        # print("{}".format(new_line))

    # print(f"nb resources not found = {nb_not_found}")
