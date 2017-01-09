import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //init
    val conf = new SparkConf()
      .setAppName("one-bucket-theta-spark")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().getOrCreate

    //Load (tpcds generated) data
    //todo: make use the smaller dataset is always "s" as theorem detection methods depend on that.
    val storeDF = spark.read.parquet(args(1))
    val caDF = spark.read.parquet(args(2))
    //DF to RDD
    val storesRdd = storeDF.rdd
    val caRdd = caDF.rdd
    val workerCount: Integer = args(0).toInt

    if (this.isTheorem2Case(storesRdd, caRdd, workerCount)) {
      //Map using zip as key
      val mappedStores = storesRdd.map(storeRow => (storeRow.get(25), storeRow))
      val mappedCa = caRdd.map(caRow => (caRow.get(9), caRow))

      val partitionedCa = mappedCa.partitionBy(new RandomPartitioner(workerCount))
      partitionedCa.persist
      val pathToExport = "/media/joinOutput/out"
      //joinOutput folder must exist and have -777 for all new children folders
      val joinResult = partitionedCa.join(mappedStores)//.coalesce(1)
      this.logger.info("Join result tuple count is approximately : " + joinResult.count)

      //Save output
      try {
        joinResult.saveAsTextFile(pathToExport)
        this.logger.info("Saved join result tuples to : " + pathToExport)
      } catch {
        case e : Exception => this.logger.error("Couldn't save file")
      }
    } else {
      this.logger.error("Couldn't determine theorem. Aborting launch...")
    }

    this.logger.info("Pausing execution... Pres any key to resume/terminate job")
    readChar()
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
