# docker-importer
Import data from external data sources into the portal.

The docker-importer is a docker container which has functionalities for metadataimport.
Currently the following sources are supported:
* Wikidata
* zbMATH
* CRAN
* arXiv
* polyDB
* crossref
* Zenodo
* ORCID

## Documentation

The importer is configured as the python package *mardi_importer* installed in docker container that can run cronjobs to schedule the import tasks. 

The documentation for the package mardi_importer is available at https://mardi4nfdi.github.io/docker-importer

The importer interacts with the wikibase instance deployed at https://portal.mardi4nfdi.de using the [mardiclient](https://github.com/MaRDI4NFDI/mardiclient) package.

## Basic requirements
* Imports should run at least once a day and import only new data
* The importer shall be extendable, i.e. imports from different sources should be possible
* Ideally, import operations should be switchable by configuration, i.e. without editing the program
* If an import doesn't succeeed, a rollback should be possible

## Configuring the import
Copy config/import_config.config.template to config/import_config.config and edit.

## Setup for local development
Create a local image: 
```
docker build -t ghcr.io/mardi4nfdi/docker-importer:main .
```
Update `portal-compose-dev.yml` or `portal-compose.override.yml` in the `portal-compose` folder with the appropriate volume route to link to the `mardi_importer` folder, e.g.
```
importer:
    restart: ${RESTART}
    volumes:
    - ../docker-importer/mardi_importer:/mardi_importer:ro
```

## Sphinx documentation
The documentation of the `mardi_importer` package is updated for every push to main running 
`make html` in `docs/` and deploying to the `gh-pages` branch, which is then published at
https://mardi4nfdi.github.io/docker-importer

## Local testing of python modules
First install the requirements from `requirements.txt`,
```
pip install -r requirements.txt
```
Then install the python package bundle ("mardi-importer") via
```
pip install -U -e .
```
`-U` enforces reinstalling the package, with `-e` modifications in
the source files are automatically taken into account.

*Note*: it is recommended (when not using docker) for local installations to use [virtual environments](https://docs.python.org/3/tutorial/venv.html).


To run the tests, do:
```
docker exec -ti mardi-importer /bin/bash /tests/run_tests.sh
```