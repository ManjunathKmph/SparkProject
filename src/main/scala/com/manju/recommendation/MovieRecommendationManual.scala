package com.manju.recommendation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import org.apache.spark.ml.evaluation.RegressionEvaluator 
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

/*
 * Class finds the recommended movies for the user.
 * Manual approach - hardcoded my movies along with ratings.
 */
object MovieRecommendationManual {
  
  val myMovies = List()
  
  /*
   * Method converts the String value by Spliting using '::' character into
   * Rating Class.
   */
  def parseRating(str: String): Rating = {  
    val fields = str.split("::")  
    assert(fields.size == 4)  
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat) 
  }
  
   /*
   * Method maps the ten hard coded movies to my user id 
   * and creates the list of Rating objects
   */
  def createUserRatingsForMyMovies(myUserId:Int):ListBuffer[Rating] = {
    val myMovieRatings = ListBuffer[Rating]();
    myMovieRatings += Rating(myUserId, 1240, 4) //Terminator, The (1984)
    myMovieRatings += Rating(myUserId, 1036, 4.5) //Die Hard (1988)
    myMovieRatings += Rating(myUserId, 1052, 4) //Proprietor, The (1996
    myMovieRatings += Rating(myUserId, 1101, 3) //Top Gun (1986)
    myMovieRatings += Rating(myUserId, 1371, 3.5) //Star Trek: The Motion Picture (1979)
    myMovieRatings += Rating(myUserId, 1483, 3.5) //Crash (1996)
    myMovieRatings += Rating(myUserId, 1487, 2) //Selena (1997)
    myMovieRatings += Rating(myUserId, 2645, 3) //Dracula (1958)
    myMovieRatings += Rating(myUserId, 2617, 4) //Mummy, The (1999)
    myMovieRatings += Rating(myUserId, 2711, 2.5) //My Life So Far (1999)
    return myMovieRatings
  }
  
  /*
   * Main method where the program execution begins
   */
  def main(args: Array[String]) {
    //Loading the Spark Context Object along with Conf object
    val conf = new SparkConf().setAppName("Movie Recommendation Manual way of adding my movies along with ratings").setMaster("local")
    val sc = new SparkContext(conf)
    
    //Loading the ratings file from HDFS
    val ratingsRdd = sc.textFile("/data/ml-1m/ratings.dat")
    val ratings = ratingsRdd.map(parseRating)
    ratings.persist
    
    //Fetch maximum userid from the ratings and add one to it.
    val userIdsList = ratings.map(rating => rating.user)
    val myUserId = userIdsList.max + 1
    
    //Mapping sample movie ids into my userId along with random ratings
    val myMovieRatings = createUserRatingsForMyMovies(myUserId);
    myMovieRatings.foreach(println)
    
    //Merging my movie ratings along with other ratings in order to do movie recommendation for my self
    val myMovieRatingsRdd = sc.parallelize(myMovieRatings)
    val mergedRatings = ratings.union(myMovieRatingsRdd)
    
    //Creating ALS class from spark mlib package in order to train the model.
    val model = ALS.trainImplicit(mergedRatings, 8, 10)
    
    //Calling recommendProducts method to fetch the recommended movies for my user id.
    val recommendedMovies = model.recommendProducts(myUserId, 20);
    
    //Loading movies.dat to fetch the movie names
    val moviesRdd = sc.textFile("/data/ml-1m/movies.dat")
    val movies = moviesRdd.map(line => {
      val fields = line.split("::")
      (fields(0).toInt, fields(1))
    }).collectAsMap()
    
    //Finally printing the movies recommended for my user Id.
    recommendedMovies.foreach { rating => println("Movie Id : " + rating.product + " -> " + movies(rating.product)) }
  }
}