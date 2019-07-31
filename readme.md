# Script to backup all transport.data.gouv.fr resources to s3

## Install

You need to install [Pipenv](https://docs.pipenv.org/en/latest/) to run the script:

`pip install --user pipenv`


## Running

There are several endpoints to the script.

### Backuping all the resources

```bash
pipenv run invoke backup-resources --api-key=<api_key> --secret-key=<secret_key>
```

* `--api-key` being the clever cloud `Key ID` of the cellar
* `--secret-key` being the clever cloud `Key Secret` of the cellar
  
### Listing all the resources in s3

```bash
pipenv run invoke list-resources --api-key=<api_key> --secret-key=<secret_key>
```

* `--api-key` being the clever cloud `Key ID` of the cellar
* `--secret-key` being the clever cloud `Key Secret` of the cellar
  

