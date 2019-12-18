# Simple DSE Example

## Introduction
This simple Java application will write data to a 
CQL table.  It generates random data at a default
rate of 10 records/sec for a default of 100 records.

This application is designed to work with simple
username/password authentication or with the creds.zip
credentials from DataStax Apollo.

## Setup
In order to use this application, the keyspace and table
needs to be created.  This can be done via cqlsh:

```
CREATE KEYSPACE IF NOT EXISTS simpleapp WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
CREATE TABLE IF NOT EXISTS simpleapp.data(store TEXT, item TEXT, quantity INT, order_time BIGINT, PRIMARY KEY ((store), order_time));
```

## Running
To run this app, run:
```
java -jar /ex_simple-1.0-SNAPSHOT-jar-with-dependencies.jar <options>
```

For non-Apollo clusters, you should supply the following options:
```
java -jar /ex_simple-1.0-SNAPSHOT-jar-with-dependencies.jar -u <username> -p <password> -h <host/IP> -d <datacenter>
```

For Apollo clusters, download the creds.zip and then supply the following options:
```
java -jar /ex_simple-1.0-SNAPSHOT-jar-with-dependencies.jar -u <username> -p <password> -c <path_to_creds.zip>
```

