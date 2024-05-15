import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Main {
  def solution(spark: SparkSession, docIds: Seq[String]) {
    import spark.implicits._

    println("Query words:")    
    for(docId <- docIds) {
      println(docId)      
    }
    val docwordIndexFilename = "Assignment_Data/docword_index.parquet"
   
    // TODO: *** Put your solution here ***
    
    val datadf =  spark.read.parquet(docwordIndexFilename)
    // datadf.createOrReplaceTempView("df")
    // val df_max = spark.sql("SELECT docID, word, max(count) FROM df GROUP BY df.docID")

    val df = datadf.select($"word", $"docID", $"count")
    df.cache()
    val temp = df.groupBy($"docID").max("count").toDF("newID", "max_count")

    //Then join 2 tables to geextract the word and drop duplicate columns as what I done in task 3b
    val joinDF = temp.join(df, $"max_count" === $"count").drop("max_count", "docID")
    
    //for(docId <- docIds) {joinDF.filter($"docID" === docId).show()}

    println("The result we found are:")

    for(docId <- docIds) {val x_show = joinDF.filter($"docID" === docId).collect()  
                          for(vl <- x_show){println(vl)} }

    
    // test with spark-shell: 
    // val IDs = Seq("3", "2")
    // datadf.select($"word", $"docID", $"count").where($"docID" === "3").show()

    // for(id <- IDs){datadf.filter($"docID" === id).show()} => that works for data has been grouped
  
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3c")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark, args)
    // Stop Spark
    spark.stop()
  }
}
