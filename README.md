# Spark Machine Learning
Set of independent machine learning examples operating on publicly available data like MovieLens Dataset

## Try it out!

You can use sbt tool 
```
sbt run
```

or generate Eclipse project and next import it
```
sbt eclipse
```


### Collaborative Filtering recommendation engine
ALSCollaborativeFilteringRecApp - Spark/scala ALS based CF recommender. 

If you want personalize add the following parameters:
```
--userId <new user Id default 0>
```

### Collaborative Filtering recommendation engine with Redis store
CFRecommendationExample - The same logic as ALSCollaborativeFilteringRecApp but store result in Redis database

Default Redis url is: redis://localhost:6379/1 
Default new user Id is: 0

If you want personalize add the following parameters:
```
--redis redis://<host>:<port>/<database number> --userId <new user Id>
``` 

More info on [Wiki](https://github.com/jacekrozwadowski/SparkMachineLearning/wiki)

### Content Based recommendation engine
TFIDFContentBaseRecApp.scala - Spark/scala TF-IDF based CB recommender. It is using the following transformation transits
Term Frequency(TF) -> Inverse Document Frequency(IDF) -> Cosine Similarity(CS)

If you want personalize add the following parameters:
```
--givenIndex <consider index default 1>
```

### Spark Sql with Cassandra integration. 
SparkSqlWithCassandraApp.scala - A few examples demonstrate simple analyze of imaginary clients transactions. In first part clients and their transactions are generating and store in Cassandra. In second part is perform simple analyze - current balance of account, clients country statistics and at the end suspicious transactions. All results are storing in Cassandra.

For running this example you have to have up and running Cassandra database on local host. No parameters are required. 

