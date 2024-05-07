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
      csv("Assignment_Data/docword.txt").
      as[Docword] // stored as a Dataset
    val vocab = spark.read.
      schema(Encoders.product[VocabWord].schema).
      option("delimiter", " ").
      csv("Assignment_Data/vocab.txt").
      as[VocabWord] //stored as a dataset

    // TODO: *** Put your solution here ***

    // Check the Schema of DataFrane: --docwords.printSchema
    // docwords.show(5)

    // as a Dataset, that is easier to retreive the data. For example, 
    //scala> docwords.map(f => f.docId).first
    // res3: Int = 3 

    // To use SQL and hive, must register Datframes of vocab and docwords as table databse
    val pairDF = vocab.as("df1").join(docwords.as("df2"), $"df1.vocabId" === $"df2.vocabId")

    val result = pairDF.groupBy($"word").sum("count").orderBy($"word")
    // spark.sql(""" SELECT word, SUM(COUNT) AS sum_count FROM vocabs INNER JOIN doc_word ON vocabs.vocabID = doc_word.vocabID GROUP BY word """).show(5)

    result.write.mode("overwrite").csv("Task_3a-out.csv")



  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3a")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}
