import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// Define case classes for input data
case class Docword(docId: Int, vocabId: Int, count: Int)
case class VocabWord(vocabId: Int, word: String)

object Main {
  def solution(spark: SparkSession) {
    import spark.implicits._
    import org.apache.spark.sql.Encoders
    // Read the input data
    val docwords = spark.read.
      schema(Encoders.product[Docword].schema).
      option("delimiter", " ").
      csv("Assignment_Data/docword-small.txt").
      as[Docword]
    val vocab = spark.read.
      schema(Encoders.product[VocabWord].schema).
      option("delimiter", " ").
      csv("Assignment_Data/vocab-small.txt").
      as[VocabWord]
    val docwordIndexFilename = "Assignment_Data/docword_index.parquet"
    // TODO: *** Put your solution here ***

    val docDF = docwords.toDF("docID", "vocabID_1", "count") //change collumn names
    val joinDF = docDF.join(vocab, $"vocabID_1" === $"vocabId").drop($"vocabID_1")

    //create the udf to extract the 1st letter of word
    import org.apache.spark.sql.functions.udf

    def get_1st_letter(x: String): String = {x(0).toString}
    val get_letterUDF = spark.udf.register[String, String]("get_1st_letter", get_1st_letter)

    
    // val result = joinDF.select($"word", $"docId", $"count", get_letterUDF($"word")).show(10)

    val result = joinDF.withColumn("firstLetter", get_letterUDF($"word")).orderBy($"word")
    result.show(10)  

    // Save the results in parquet
    result.write.mode("overwrite").partitionBy("firstLetter").format("parquet").save(docwordIndexFilename)


  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3b")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}
