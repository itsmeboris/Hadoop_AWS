# Hadoop with AWS
## bi-gram likelihood
### Assignment for course DSP202

In this assignment I will code a real-world application of Map-Reduce using Hadoop and aws. 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.  
> You can build this jar using the pom file provided

### Prerequisites

* Java 1.8 and higher  
* Maven
* Setting up a local HDFS on your machine, i suggest following this guide:  [Install Hadoop](https://kontext.tech/column/hadoop/377/latest-hadoop-321-installation-on-windows-10-step-by-step-guide)
> make sure to edit start.cmd using notepad and change line cd <PROJECT_DIRECTORY> to your local project directory

## Deployment

* To Run local application simply make the jar using  
> mvn package  
* To start your local HDFS and run the application run 
> start.cmd  
this will save the file to output.txt in your project folder
* you can run it manually using
> %HADOOP_HOME%/bin/hadoop jar target/Ex2-1.0-SNAPSHOT-jar-with-dependencies.jar  
this will launch the application on the input files and will output to /user/output/part-r-00000
* when you want to close all connection and HDFS run
> stop.cmd

## Built With
* [Maven](https://maven.apache.org/) - Dependency Management

## Authors

* **Boris Sobol**

## License

This project is licensed under the Apache License - see the [LICENSE.md](LICENSE.md) file for details

## Additional information
