import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Main {
  def solution(spark: SparkSession, queryWords: Seq[String]) {
    import spark.implicits._

    println("Query words:")
    for(queryWord <- queryWords) {
      println(queryWord)
    }
    val docwordIndexFilename = "Assignment_Data/docword_index.parquet"    
    
    // TODO: *** Put your solution here ***


    // Test with spark-shell
    // val word_lists = Seq("look", "at", "car", "power", "find", "truck")

    println("The most occurences of word(s) we found:")
    for(word <- queryWords) {val temp_char = word(0).toString()

                             val datadf =  spark.read.parquet(docwordIndexFilename)

                             val temp_word = datadf
                                            .orderBy($"count".desc)
                                            .filter($"firstLetter" === temp_char)
                                            .select($"docID", $"word")
                                            .where($"word" === word)
                                            .take(1)

                             for(w <- temp_word) {println(w)}                            
                            
                            }

  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3d")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark, args)
    // Stop Spark
    spark.stop()
  }
}
