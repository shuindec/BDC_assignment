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
    
    // get the colum _balance_ then store to get_col
	val get_col = bankdata.map(line => line(5))
	// convert to interger list
	val balance_data = get_col.map(_.toInt)
	
	val small = balance_data.filter(_ < 500).count
	val med = balance_data.filter( x => x > 500 && x < 1500).count
	val hi = balance_data.filter(_ > 1500).count

	
	val result = sc.parallelize(List(("Low", small), ("Medium", med), ("High", hi)))
	
	result.saveAsTextFile("Task_1c-out")


  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task1c")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.default.parallelism", 1)
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}
