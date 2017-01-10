import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val logger: Logger = Logger.getLogger(this.getClass)
  val pathToExportResults = "/media/joinOutput/out"

  def main(args: Array[String]): Unit = {
    //init
    val conf = new SparkConf().setAppName("one-bucket-theta-spark")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().getOrCreate

    //Load data
    val storeDF = spark.read.parquet(args(1))
    val caDF = spark.read.parquet(args(2))

    //DF to RDD
    val storesRdd = storeDF.rdd
    val caRdd = caDF.rdd
    val workerCount: Integer = args(0).toInt

    if (this.isTheorem2Case(storesRdd, caRdd, workerCount)) {
      val mappedCa = caRdd.map(caRow => (caRow.get(0), caRow))
      val partitionedCa = mappedCa.partitionBy(new RandomPartitioner(workerCount))

      //Anagkastika map gia na mporesw na xrhsimopoihsw ton RandomPartitioner
      val cartesian = partitionedCa.cartesian(storesRdd)

      //Epanafora se morfh [store, customer_address]
      val properCartesian = cartesian.map(c => (c._1._2, c._2))

      val filteredCartesian = properCartesian
        .filter(c => c._1.getAs("ca_zip") != null)
        .filter(c => !(c._1.getAs("ca_zip").asInstanceOf[String].substring(0, 4) == c._2.getAs("s_zip").asInstanceOf[String].substring(0, 4)))

      //      logger.info("filter cartesian count : " + filteredCartesian.count())

      //Try to save results to file
      try {
        filteredCartesian.saveAsTextFile(pathToExportResults)
        this.logger.info("Saved theta join result tuples to : " + pathToExportResults)
      } catch {
        case e: Exception => this.logger.error("Couldn't save file. Message is : " + e.getMessage)
      }
    } else {
      this.logger.error("Input data dont match theorem 2 case. Aborting execution...")
    }

    readChar() //Pauses execution to allow for inspection
  }

  //Theorem 2 : |S| < |T|/r
  def isTheorem2Case(s: RDD[Row], t: RDD[Row], r: Integer): Boolean = {
    //todo: dokimase an xtipaei ram se megala dataset kai vale countApprox
    val sSize = s.count
    val tSize = t.count

    if (sSize < tSize / r) {
      this.logger.info("Theorem 2 case detected")
      return true
    }
    return false
  }
}
