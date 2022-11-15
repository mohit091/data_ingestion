# data_ingestion

# Project Overview

This project is to ingest the data available in file to database so that we can run our analytics queries on top of it.


# Arcitecture

Used docker images of postgres as database to load the data and docker image of python to do the ingestion processing . Both the images are coupled with docker compose to start the services. When docker compose is start it first start the db image and then the python image . We can then login to the database image to see the ingested data and run our reqyired analytics queries to see the result. 


Python code is used to ingest the data from the available file first to datalake and then using datalake we ingest data to the data model comprising of facts and dimensions



# Set up 

1. clone the repo from git
2. set up docker in local machine
3. go to the data ingestion folder using->  cd data_ingestion
4. run the docker compose file using->  docker-compose up --build . It will start all the services (db and ingestion process)
5. To login to database and see the inserted data or run any other analytics quries 
6. run docker ps command : copy the container id from the result of this command
7. run docker exec -it <container id> bin/bash. This will open the database docker image.
8. login to database using psql postgres://username:secret@localhost:5432/database
9. Now you can start running your queries
