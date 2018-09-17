# twitter-spark-oracledb
Pull tweets into Spark Streaming and store in Oracle Database

**this is a work in progress :)**

## running 
1. create a config file 
  ```json
  {
    "broker": "123.456.789.0:6667",
    "topic": "my-sweet-topic-conga",
    "consumerKey": "*************",
    "consumerSecret": "**************************************************",
    "accessToken": "**************************************************",
    "accessTokenSecret": "*********************************************",
    "dbUser": "system",
    "dbPassword": "oracle",
    "dbConnString": "jdbc:oracle:thin:@localhost:1521:xe"
  }
  ```
2. place said config file somewhere that spark can access it 
3. add the config file location to `run.sh`, right now it's set to `/tmp/config.json` (see below)
```bash
config_file="/tmp/config.json"
```
4. run it :) 

## dev
using https://hub.docker.com/r/sath89/oracle-12c/` for my test database (connection details given in the example config)

to remotely debug (I'm using intellij) just uncomment the `SPARK_SUBMIT_OPTS`
```bash
# # uncomment this line to run in remote debugging mode
#export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

## many thanks to 
http://bahir.apache.org/docs/spark/2.0.0/spark-streaming-twitter/
http://twitter4j.org/oldjavadocs/4.0.4/index.html
https://spark.apache.org/docs/2.1.0/ 