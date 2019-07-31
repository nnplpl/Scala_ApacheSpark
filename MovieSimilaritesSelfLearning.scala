package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charaset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt

object MovieSimilarites{

	/*Load up a Map of movie IDs to movie names*/
	def loadMovieNames() : Map[Int, String] = {

		//Handle character encoding issues
		/*we made codec a implicit parameter, it means if we don't provide a related paramater, the method will*/
		/*automatically use the value with "implicit" as parameter.*/
		implicit val codec = Codec("UTF-8")
		/* onMalformedInput is used to change all input whose type is not UTF-8 to UTF-8*/
		/* onUnmappableCharacter is used to change the type of unmappable values to UTF-8*/
		codec.onMalformedInput(CodingErrorAction.REPLACE)
		codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

		//Create a Map(type) of Ints to Strings, and populate it from u.item
		var movieNames: Map[Int, String] = Map()
		
		val lines = Source.fromFile("../ml-100k/u.item").getlines()
		for (line <- lines){
			var fields = line.split('|')
			if (fields.length > 1) {
				(movieNames += line(0).toInt -> fields(1))
			}
		}
		return movieNames
	} 

	type MovieRating = (Int, Double)
	type UserRatingPair = (Int, (MovieRating, MovieRating))

	def makePairs(userRatings: UserRatingPair) = {
		val movieRating1 = userRatings._2._1
		val movieRating2 = userRatings._2._2

		val movie1 = movieRating1._1
		val rating1 = movieRating1._2
		val movie2 = movieRating2._1
		val rating2 = movieRating2._2

		((movie1, movie2),(rating1, rating2))
		/*eg: ((Star Wars, Spider Man),(4.0 , 4.1)*/
	}

	type RatingPair = (Double, Double)
	type RatingPairs = Iterable[RatingPair]

	def filterDuplicates(userRatings: UserRatingPair) = {
		val movieRating1 = userRatings._2._1
		val movieRating2 = userRatings._2._2

		val movie1 = movieRating1._1
		val movie2 = movieRating2._1
		/* return boolearn result, only the right and no duplicate values will be marked True*/
		return movie1 < movie2
		/*Filter out the duplicate pairs like we have moive A, B*/
		/*After self-join AA, AB, BA, BB will appear, only AB will remain as others don't satisfy movie1< movie2*/

	} 

	def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
		var numPairs: Int = 0
		var sum_xx: Double = 0.0
		var sum_xy: Double = 0.0
		var sum_yy: Double = 0.0

		for (pair <- ratingPairs) {
			val ratingX = pair._1
			val ratingY = pair._2

			sum_xx += ratingX * ratingX
			sum_yy += ratingY * ratingY
			sum_xy += ratingX * ratingY
			numPairs += 1
		}

		val numrator: Double = sum_xy
		val denominator = sqrt(sum_xx) * sqrt(sum_yy)

		var score: Double = 0.0
		if (denominator != 0){
			score = numrator / denominator
		}

		return (score, numPairs)
	}

	def main(args: Array[String]) {
		Logger.getLogger("org").setlevel(level.ERROR)

		val sc = new SparkContext("local[*]","MovieSimilarites")

		println("\n Loading Movie Names")

		val nameDict = loadMovieNames()

		val data = sc.textFile("../ml-100k/u.data")

		// Map ratings to key / value pairs: user ID => movie ID, rating
		val ratings = data.map(l => l.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

		//Emit every movie rated together by the same user
		//self-join to find every combination
		val joinedRatings = ratings.join(ratings)

		//At this point our RDD consists of UserID => ((movieID, rating), (movieID, rating))
		val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

		//Now key by (movie1, movie2) pairs
		val moviePairs = uniqueJoinedRatings.map(makePairs)

		//Now we have (movie1, movie2) => (Raitng1, Rating2) pairs
		// Can now compute MovieSimilarites
		val moviepairSimilarities = moviePairRatings.mapvalue(computeCosineSimilarity).cache()

		//Save the result if desired
		//val sorted = moviepairSimilarities.sortByKey()
		//sorted.saveAsTextFile("movie-sims")

		//Extract similarities for the movie we care about that are "good"
		if (args.length > 0) {
			val scoreThreshold = 0.97
			val coOccurenceThreshold = 50.0

			val movieID:Int = args(0).toInt

			//filter for movies with this sim that are "good" as defined by
			//our quality thresholds above

			val filteredResults = moviePairsSimilarities.filter( x => 
				{
					val pair = x._1
					val sim = x._2
					(pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
				}
			)
		
	      // Sort by quality score (false means for descending order).
		    val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)
		      
		    println("\nTop 10 similar movies for " + nameDict(movieID))
		    for (result <- results) {
		        val sim = result._1
		        val pair = result._2
		        // Display the similarity result that isn't the movie we're looking at
		        var similarMovieID = pair._1
		        if (similarMovieID == movieID) {
		          similarMovieID = pair._2
		        }
		        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
	        }
	    }
	}
}















