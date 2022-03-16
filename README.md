# docker-importer
Import data from external data sources into the portal.

The docker-importer is a docker-container which has functionalities for data-import (e.g. from swMATH, zbMATH), 
and can trigger the import of data in a wikibase container in the same docker-composition cyclically. 
The import scripts in the importer are written in python and examples for these scripts can be seen as 
Jupyter-Notebooks in the repository Portal-Examples.

## Documentation
UML activity (todo:class) diagrams are in the `doc` folder. 
* .drawio files can be edited using http://draw.io
* .svg files can be viewed using a browser

## Basic requirements
* Imports should run at least once a day and import only new data
* The importer shall be extendable, i.e. imports from different sources should be possible
* Ideally, import operations should be switchable by configuration, i.e. without editing the program
* If an import doesn't succeeed, a rollback should be possible

## Testing
```
docker-compose -f docker-compose.yml up -d
```
Will :
* start a minimal wikibase setup (http://localhost:8080), 
* rebuild the importer image and start the mardi-importer container

To run the tests, do:
```
docker exec -ti mardi-importer /bin/bash /tests/run_tests.sh
```
