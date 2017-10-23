import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

import java.net.URL
import java.io.{File,IOException}

import com.redis._
import serialization._

import com.google.gson.Gson
import java.net.URI

object CFRecommendationExample {

  /*User Class*/
  case class Movie(movieId: Int, title: String, genre: String)
  case class TopMovie(movieId: Int, title: String, rating: Double)
  
  
  /*Utils*/
  //Create Movie object from input string
  def parseMovie(str: String): Movie = {
    val fields = str.split(',')
    Movie(fields(0).toInt, fields(1).toString, fields(2).toString)
  }
  
  //Get Redis dabatase number from given url
  def extractDatabaseNumber(connectionUri: java.net.URI): Int = {
    Option(connectionUri.getPath).map(path =>
      if (path.isEmpty) 0
      else Integer.parseInt(path.tail)
    )
      .getOrElse(0)
  }
  
  //Create Redis Client from given url
  def getRedisClient(redisUrl: String): RedisClient = {
    val uri = URI.create(redisUrl)
    val host = uri.getHost
    val port = uri.getPort
    val database = extractDatabaseNumber(uri)
    println("Connecting to: "+ host + ":" + String.valueOf(port) + "/" + database)
    
    new RedisClient(host, port, database)
  }
  
  
  /*Main CF Engine*/
  class CollaborativeFilteringEngine(redisUrl: String){
    
    //Create local Spark context
    var conf: SparkConf = _
    var sc: SparkContext = _
    
    
    /*Main RDD used in class.
     * Rating: userId, movieId, rate
     * Movie: movieId, title, genre
     * 
     * */
    var ratings: RDD[Rating] = _
    var movies: RDD[Movie] = _
    
    //Create Spark context
    def connectToSpark() = {
      conf = new SparkConf().setMaster("local").setAppName("local").set("spark.testing.memory", "471859200")
      sc =  new SparkContext(conf)
      sc.setLogLevel("ERROR")
      
      ratings = sc.emptyRDD
      movies = sc.emptyRDD
    }
    
    //Load data from text files into Spark RDD's
    def loadData(ratingFile: String, movieFile: String) = {
      println("Loading")
      val ratingResource = this.getClass.getClassLoader.getResource(ratingFile)
      val ratingDataFile = new File(ratingResource.toURI).getPath
    
      val movieResource = this.getClass.getClassLoader.getResource(movieFile)
      val movieDataFile = new File(movieResource.toURI).getPath
      
      val ratingData = sc.textFile(ratingDataFile)
      val movieData = sc.textFile(movieDataFile)
      
      ratings = ratingData.map(_.split(',') match { case Array(user, item, rate, timestamp) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
      })
      movies = movieData.map(parseMovie)
      
    }
    
    
    //Select the best rank parameter for given data set
    def selectBestRank(trainingSplit : Double): Int = {
      println("Selection")
      
      //Split data on trainign and test set
      val Array(training, test) = ratings.randomSplit(Array(trainingSplit, 1-trainingSplit))
      
      // Evaluate the model on rating data
      var bestRank = 0
      var bestRMSE = Double.MaxValue
      for (i <- List(4,6,8,10,12)) {
        
        // Build the recommendation model using ALS
        val rank = i
        val numIterations = 5
        val ratingsModel = ALS.train(training, rank, numIterations, 0.1)
        
        //prepare prediciton data - take out rate
        val usersProducts = test.map { case Rating(user, product, rate) =>
          (user, product)
        }
      
        //perform prediction
        val predictions = ratingsModel.predict(usersProducts).map { case Rating(user, product, rate) =>
            ((user, product), rate)}
        
        //Join prediction with real data for comparison
        val ratesAndPreds = test.map { case Rating(user, product, rate) =>
          ((user, product), rate)
        }.join(predictions)
      
        /*Calculate Root Mean Squared Error
         * for every case we calculate difference between predicted and real value: val err = (r1 - r2)
         * calculate square error:  err * err
         * calculate mean of square error
         * calculate root of previouse value
        */
        val RMSE = math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
          val err = (r1 - r2)
          err * err
        }.mean())
      
        println("Rank = " + rank)
        println("Root Mean Squared Error = " + RMSE)
        
        if(RMSE<bestRMSE){
          bestRMSE = RMSE
          bestRank = rank
        }
      }
      
      println("The Best Rank = " + bestRank)
      println("The Best RMSE = " + bestRMSE)
      
      //return
      bestRank
    }
    
    //
    def train(userId: Int, newUserRating: List[(Int, Int)], rank: Int) {
      println("Training userId: "+userId)
      
      //create RDD
      var list = new ListBuffer[Rating]()
      newUserRating.foreach{case(productId, rate) => list += Rating(userId, productId.toInt, rate.toDouble) }
      val newUserRatingDF = sc.parallelize(list)
      
      //join both data set
      val newRatings = ratings.union(newUserRatingDF)
      
      //Build the recommendation model using ALS
      val numIterations = 5
      val newRatingsModel = ALS.train(newRatings, rank, numIterations, 0.1)
      
      //Prediction prepare
      val usersProducts = newRatings.map { case Rating(user, product, rate) =>
        (user, product)
      }
      
      //Prediction
      val predictions =
      newRatingsModel.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
      
      //Join real rate with prediction
      val ratesAndPreds = newRatings.map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }.join(predictions)
      
      //Recommend
      val recommendedProducts = sc.parallelize(newRatingsModel.recommendProducts(userId, 10));
      
      //Get recommended movies and rate
      val tab1 = recommendedProducts.map { case Rating(user, movieId, rate) => (movieId.toInt, rate)}
    
      //Get movies title
      val tab2 = movies.map {case Movie(movieId, title, genre) => (movieId.toInt, title) }
    
      //Join both
      val finalRec = tab1.join(tab2)
      
      //Get top 10
      val top10Movies = finalRec.sortBy(_._2._1, ascending=false).take(10)

      //Get Connection to Redis and flush database
      val _r = getRedisClient(redisUrl)
      _r.flushdb
      
      //Store prediction in Redis
      val gson = new Gson
      var movieList = ListBuffer[TopMovie]()
      top10Movies.zipWithIndex.foreach{ case (item, index) => 
        val movie = TopMovie(item._1, item._2._2, item._2._1)
        movieList += movie
        _r.zadd("cfr:movie:"+userId, movie.rating,  gson.toJson(movie))
      }
      
      println("Top 10 Predicted Movies: ")
      movieList.foreach(movie => println("	"+movie))
      
    }
    
    //Prediction user preferred movies
    def predict(userId: Int) {
      println("Prediction")
      
      //Redis   
      val _r = getRedisClient(redisUrl)
      
      //Create Json parser
      val gson = new Gson
      
      //Retrieve data from Redis
      var movieList = ListBuffer[TopMovie]()
      for(json <- _r.zrange("cfr:movie:"+userId, 0, 9, RedisClient.DESC).get) {
        val movie = gson.fromJson(json, classOf[TopMovie])
        movieList += movie
      }
      
      println("Top 10 Predicted Movies(Reclamation): ")
      movieList.foreach(movie => println("	"+movie))
      
    }
    
    def stopSpark(){
      sc.stop()
    }
    
  }
  
  
  def main(args: Array[String]): Unit = {
    
    var redisUrl = "redis://localhost:6379/1"
    var userId = 0
    
    args.sliding(2, 2).toList.collect {
      case Array("--redis", argRedis: String) => redisUrl = argRedis
      case Array("--userId", argUserId: String) => userId = argUserId.toInt
    }
    
    //1. Create CF engine
    val cfEngine = new CollaborativeFilteringEngine(redisUrl)
    
    //2. Connect to Spark
    cfEngine.connectToSpark()
    
    //3. Load data
    cfEngine.loadData("ratings.csv", "movies.csv")
    
    //4. Train model and get the best rank value
    val rank: Int = cfEngine.selectBestRank(0.75)
    
    //5. Prepare data for new user
    val newUserRating: List[(Int, Int)] = List(
       (260,4), // Star Wars (1977)
       (1,3),   // Toy Story (1995)
       (16,3),  // Casino (1995)
       (25,4),  // Leaving Las Vegas (1995)
       (32,4),  // Twelve Monkeys (a.k.a. 12 Monkeys) (1995)
       (335,1), // Flintstones, The (1994)
       (379,1), // Timecop (1994)
       (296,3), // Pulp Fiction (1994)
       (858,5) , // Godfather, The (1972)
       (50,4)  // Usual Suspects, The (1995)
    )
    
    //6. Train model using new user data
    cfEngine.train(userId, newUserRating, rank)
    
    //7. Stop Spark engine
    cfEngine.stopSpark
    
    //8. Predict Top 10 movies for user
    cfEngine.predict(userId)
    
  }
}