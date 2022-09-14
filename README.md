## Code setup and Installation
There are multiple ways to run the app:
### Running the app with prebuilt jar: 
```
java -DXmx=32g -Dspring.datasource.hikari.maximumPoolSize=100 -jar yb-workload-simu-app.jar

-Dnode=<database-host-name> [default: 127.0.0.1]
-Ddbuser=<userid> [default: yugabyte]
-Ddbpassword=<password> [default: yugabyte]
-Dport=<port> [default: 5433]
-Dmax-pool-size=<max-pool-size> [default: 100]
-Ddbname=<dbname> [default: yugabyte]
-Dspring.profiles.active=<profile> [default: application.yaml]
-Dserver.port=8081 [default: 8080]
-DidCounter=1 [default: 1]
-Dssl=true [default: false]
-Dsslmode=verify-full [default: disable]
-Dsslrootcert=<certificatepath> 
-Dworkload=genericWorkload
```

### Additional parameters if you wish to run YCQL workload
```
-Dworkload=genericCassandraWorkload
-Dspring.data.cassandra.contact-points=<host ip> 
-Dspring.data.cassandra.port=9042 
-Dspring.data.cassandra.local-datacenter=<datacenter> [ex. us-east-2 ]
-Dspring.data.cassandra.userid=cassandra 
-Dspring.data.cassandra.password=<cassandra-password>
```

### Running the app from Docker image (Intuit Demo)
```
docker pull yugabytedb/yb-workload-simu-app
```
```
sudo docker run -p 8080:8080 -e "JAVA_OPTS=-Dnode=<database-host-name> -Duser=<userid> -Dpassword=<password>" docker.io/yugabytedb/yb-workload-simu-app
```
``` 
Addition JAVA_OPTS parameters: max-pool-size, dbname, port
```

### Running the app from Docker image (Sonos Demo)
```
docker pull akscjo/yb-workload-simu-app-so
```
```
sudo docker run -p 8080:8080 -e "JAVA_OPTS=-Dnode=<database-host-name> -Duser=<userid> -Dpassword=<password>" docker.io/akscjo/yb-workload-simu-app-so
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
# workload-simulation-demo-app
