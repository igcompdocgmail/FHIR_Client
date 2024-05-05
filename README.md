This application reads FHIR data from JSON files and sends it to a FHIR server which is protected by a reverse proxy via APISIX. 
The FHIR data is then stored on the FIRELY servers MongoDB repository. The resources are then sent to an Apache Spark application where they are processed
and then uploaded to a local mySQL server. 

This project represents the skeleton of a data pipeline that could be used for transfering medical data, conforming to FHIR standards, between several systems. 
