import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object Main {
  val logger: Logger = Logger.getLogger(this.getClass)
  val pathToExportResults = "/media/joinOutput/out"

  def main(args: Array[String]): Unit = {
    //init
    val conf = new SparkConf().setAppName("one-bucket-theta-spark")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().getOrCreate

    //Load data
    val partitionCount: Integer = args(0).toInt
    val storeDF = spark.read.parquet(args(1))
    val caDF = spark.read.parquet(args(2))
    //DF to RDD
    val storesRdd = storeDF.rdd
    val caRdd = sc.parallelize(caDF.rdd.takeSample(withReplacement = false, 16000, 1))
    //Data information
    val storesCount = storesRdd.count()
    val caCount = caRdd.count()
    val cartesianSize = storesCount * caCount

    //Apo typo sto paper -> Sto theorem 1 einai akeraia ta dimensions. Sta alla kanw rounding gia na to xeiristw
    //Todo: anti gia rounding kane inflation (theorem 3)
    val dimensionSize = BigDecimal(Math.sqrt(storesCount * caCount / partitionCount))
      .setScale(0, BigDecimal.RoundingMode.HALF_EVEN).toInt
    // --------------------------------------
    //Shuffle tis listes gia na einai random ta tuples otan parw range apo kathe lista
    val tTuples = Random.shuffle(storesRdd.collect().toList)
    val sTuples = Random.shuffle(caRdd.collect().toList)

    val hLoops = (storesCount / dimensionSize).toInt
    val vLoops = (caCount / dimensionSize).toInt
    var region = 1

    var regionalJoinObjects = new ListBuffer[RegionalJoin]

    for (i <- 1 to vLoops) {
      var verticalList = sTuples.drop(dimensionSize * (i - 1)).take((dimensionSize * i) - 1)
      for (j <- 1 to hLoops) {
        var horizontalList = tTuples.drop(dimensionSize * (j - 1)).take((dimensionSize * j) - 1)
        regionalJoinObjects += RegionalJoin(region, horizontalList, verticalList)
        region += 1
      }
    }
    //create rdd
    val rddOmg = sc.parallelize(regionalJoinObjects).map(rj => (rj.regionNumber, rj))
    // --------------------------------------
    //Try to save results to file
    try {
      //w/e -> saveAsTextFile(pathToExportResults)
      this.logger.info("Saved theta join result tuples to : " + pathToExportResults)
    } catch {
      case e: Exception => this.logger.error("Couldn't save file. Message is : " + e.getMessage)
    }

    readChar() //Pauses execution to allow for inspection
  }
}
