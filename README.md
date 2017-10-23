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


###Collaborative Filtering recommendation engine
ALSCollaborativeFilteringRecApp - Spark/scala ALS based CF recommender. 

If you want personalize add the following parameters:
```
--userId <new user Id default 0>
```

###Collaborative Filtering recommendation engine with Redis store
CFRecommendationExample - The same logic as ALSCollaborativeFilteringRecApp but store result in Redis database

Default Redis url is: redis://localhost:6379/1 
Default new user Id is: 0

If you want personalize add the following parameters:
```
--redis redis://<host>:<port>/<database number> --userId <new user Id>
``` 

###Content Based recommendation engine
TFIDFContentBaseRecApp.scala - Spark/scala TF-IDF based CB recommender. It is using the following transformation transits
Term Frequency(TF) -> Inverse Document Frequency(IDF) -> Cosine Similarity(CS)

If you want personalize add the following parameters:
```
--givenIndex <consider index default 1>
```


More info on [Wiki](https://github.com/jacekrozwadowski/SparkMachineLearning/wiki)
