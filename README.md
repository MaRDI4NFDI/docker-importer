# docker-importer
Import data from external data sources into the portal.

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
* start a minmal wikibase setup (http://localhost:8080), 
* rebuild the importer image and start the mardi-importer container

To run the tests, do:
```
docker exec -ti mardi-importer /bin/bash /tests/run_tests.sh
```
