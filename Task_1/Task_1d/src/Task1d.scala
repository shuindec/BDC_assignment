import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext) {
    // Load each line of the input data
    val bankdataLines = sc.textFile("Assignment_Data/bank.csv")
    // Split each line of the input data into an array of strings
    val bankdata = bankdataLines.map(_.split(";"))

    // TODO: *** Put your solution here ***
	val get_data = bankdata.map(line => (line(3), line(5), line(1), line(2),line(7)))

	// val data_sort1.1 = get_data.sortBy(_._1, true)
	// data_sort1.groupBy(_._1).sortBy(_._2, true) 
	
	val result = get_data.sortBy{case(school,balance,_,_,_) => (school, -balance.toInt)}
	
	
	result.saveAsTextFile("Task_1d-out")

  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task1d")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}
