from invoke import task
import requests
import boto3
import botocore
import dateutil
import logging
from pathlib import Path
import datetime

logging.basicConfig(level=logging.INFO)

WORKING_DIR = Path("./tmp")
CELLAR_URL = "https://cellar-c2.services.clever-cloud.com"


def _get_ressource_headers(ressource_url):
    head = requests.head(ressource_url)
    location = head.headers.get("location")
    if location:
        # we query the other location
        head = requests.head(location)

    return head.headers


def _get_all_ressources():
    r = requests.get("https://transport.data.gouv.fr/api/datasets")
    datasets = r.json()

    for d in datasets:
        dataset_name = d["title"]
        for r in d["resources"]:
            url = r.get("url")
            if not url:
                continue

            # headers = _get_ressource_headers(url)
            # yield {"dataset": d, "metadata": r, "headers": headers}
            yield {"dataset": d, "metadata": r}


def _get_already_backuped_resources(resource, dataset_bucket):
    return dataset_bucket.objects.filter(Prefix=_resource_title(resource))


def _needs_to_be_updated(resource, dataset_bucket):
    # we list all resources of the bucket to find if the resource is already at its latest version
    objs = _get_already_backuped_resources(resource, dataset_bucket)

    max_last_modified = max((o.last_modified for o in objs), default=None)

    if max_last_modified:
        resource_modification_date = dateutil.parser.parse(
            resource["metadata"]["updated"]
        )
        if max_last_modified >= resource_modification_date:
            logging.debug(
                f"resource is already backuped ({resource_modification_date} < {max_last_modified})"
            )
            return False

    # TODO: ETag, Last-Modified, md5sum ?
    return True


def _needs_to_be_backuped(resource):
    # for the moment we only backup the public transit
    fmt = resource["metadata"].get("format")
    t = resource["dataset"].get("type")
    return t == "public-transit" and fmt in ("GTFS", "NETEX")


def _debug_name(resource):
    return f'{resource["dataset"]["title"]} - {resource["metadata"]["title"]}'


def _resource_title(resource):
    return (
        f'{resource["metadata"]["title"]}'.replace(" ", "_")
        .replace("/", "_")
        .replace("'", "_")
    )


def _download_resource(resource) -> str:
    resource_response = requests.get(resource["metadata"]["url"], allow_redirects=True)
    now = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    file_name = f"{_resource_title(resource)}_{now}"
    file_path = path = WORKING_DIR / file_name
    open(file_path, "wb").write(resource_response.content)
    return (file_path, file_name)


def _backup(resource, dataset_bucket):
    logging.info(
        f"backuping {_debug_name(resource)} (modified {resource['metadata']['updated']}"
    )

    # we download the file
    file_path, file_name = _download_resource(resource)

    dataset_bucket.upload_file(
        Filename=str(file_path.resolve()),
        Key=file_name,
        ExtraArgs={"Metadata": {"url": resource["metadata"]["url"]}},
    )

    for o in _get_already_backuped_resources(resource, dataset_bucket):
        logging.info(
            f"resources in dataset bucket: {o.key} ({o.last_modified} -- size = {o.size} -- etag = {o.e_tag})"
        )

    # we remove the file afterward
    file_path.unlink()


def _get_bucket_id(resource):
    return f'dataset_{resource["dataset"]["datagouv_id"]}'


def _make_s3_client(api_key, secret_key):
    return boto3.resource(
        "s3",
        endpoint_url=CELLAR_URL,
        aws_access_key_id=api_key,
        aws_secret_access_key=secret_key,
        config=botocore.client.Config(
            s3={"addressing_style": "path"}, signature_version="s3"
        ),
    )


@task(default=True)
def backup_resources(ctx, api_key, secret_key=None):
    """
    backup all transport.data.gouv.fr resources to s3

    Only resources that have changed are backuped
    """
    nb_backuped_resources = nb_resources = nb_resources_to_backup = 0
    s3_client = _make_s3_client(api_key, secret_key)
    for r in _get_all_ressources():
        nb_resources += 1
        if not _needs_to_be_backuped(r):
            logging.debug(f"we don't need to backup {_debug_name(r)}")
            continue

        dataset_bucket = s3_client.Bucket(_get_bucket_id(r))

        # if nb_resources_to_backup >= 10:
        #     logging.warn("too many backuped files for a POC, we stop")
        #     return

        dataset_bucket.create()
        nb_resources_to_backup += 1

        if not _needs_to_be_updated(r, dataset_bucket):
            logging.debug(f"skipping {_debug_name(r)}, it does not need to be updated")
            continue

        nb_backuped_resources += 1
        _backup(r, dataset_bucket)

    logging.info(
        f"{nb_backuped_resources} resources backuped / {nb_resources_to_backup} (and {nb_resources} total resources)"
    )
    # cleanup removed ressource ?
    # handle max history ?


@task()
def list_resources(ctx, api_key, secret_key=None):
    """
    List all backuped ressources
    """
    s3_client = _make_s3_client(api_key, secret_key)

    for b in s3_client.buckets.all():
        logging.info(f"* bucket {b.name}")

        for o in b.objects.all():
            logging.info(
                f"  - {o.key} ({o.last_modified} -- size = {o.size} -- etag = {o.e_tag})"
            )
