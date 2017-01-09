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
      partitionedCa.join(mappedStores).collect() //kane to collect na grafei se file(?)
    }

    readChar() //Pause
  }

  // |S| < |T|/r
  def isTheorem2Case(s: RDD[Row], t: RDD[Row], r: Integer): Boolean = {
    val sSize: Integer = s.countApprox(7000, 0.95).initialValue.mean.toInt//todo: check it akrivws kanei to initialValue
    val tSize: Integer = t.countApprox(7000, 0.95).initialValue.mean.toInt

    if (sSize > tSize) {

    }

    this.logger.info("Theorem 2 case detected")
    true; //todo : grapse tous kanones kai run sto clusteraki ^_^
  }
}
