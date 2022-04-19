## Code setup and Installation
There are multiple ways to run the app:
### Running the app with prebuilt jar: 
```
java -DXmx=32g -Dspring.datasource.hikari.maximumPoolSize=100 -jar yb-workload-simu-app.jar

-Dnode=<database-host-name> [default: 127.0.0.1]
-Duser=<userid> [default: yugabyte]
-Dpassword=<password> [default: yugabyte]
-Dport=<port> [default: 5433]
-Dmax-pool-size=<max-pool-size> [default: 100]
-Ddbname=<dbname> [default: yugabyte]
-Dspring.profiles.active=<profile> [default: application.yaml]
-Dserver.port=8081 [default: 8080]
-DidCounter=1 [default: 1]
```

### Running the app from Docker image
```
docker pull yugabytedb/yb-workload-simu-app
```
```
sudo docker run -p 8080:8080 -e "JAVA_OPTS=-Dnode=<database-host-name> -Duser=<userid> -Dpassword=<password>" docker.io/yugabytedb/yb-workload-simu-app
```
``` 
Addition JAVA_OPTS parameters: max-pool-size, dbname, port
```

### Local Environment: 
```
./mvnw spring-boot:run -Dspring-boot.run.profiles=dev -DidCounter=1
```

### Prod APP UI: 
```
http://<HOSTNAME>:8080
```

### Following options are now available from UI.
##### Submission workload:
```
http://3.13.218.196:8080/api/simulate-submissions/{threads}/{numberOfSubmissions}

- Run 10 “select” queries :
  - 2 queries with join between filing and dl_filing
  - 2 queries from routing_number table.
  - 2 queries from transmission table.
  - 2 queries from transmission_filing table
  - 2 queries from transmit_data_value table
- Run 12 “inserts”:
  - Insert 2 rows to filing table
  - Insert 2 rows to dl_filing
  - Insert 2 rows to routing_number
  - Insert 2 rows to transmission
  - Insert 2 rows to transmission_filing
  - Insert 2 rows to transmit_data_value
```
##### Status Check workload:
```
http://3.13.218.196:8080/api/simulate-status-checks/{threads}/{numberOfStatusChecks}

- Run 20 “selects” from FILING table (4 on each table)
  - 4 select queries on filing table
  - 4 select queries on dl_filing
  - 4 select queries on transmission
  - 4 select queries on transmission_filing
  - 4 select queries on transmit_data_value


- Run 10 “updates”  (2 on each table)
  - 2 updates on filing
  - 2 updates on dl_filing
  - 2 updates on transmission
  - 2 updates on transmission_filing
  - 2 updates on transmit_data_value

```

##### Simulate-updates
```
http://3.13.218.196:8080/api/simulate-updates/{threads}/{numberOfTimesToUpdateSameRecord}

 - example: http://localhost:8080/api/simulate-updates/20/100000
 - This will run 20 threads and in each thread pick a random filing Id and perform  updates 100000 times on same filing id.
 ```




