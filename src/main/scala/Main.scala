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
    logger.info("tTuples count after random: " + tTuples.size + "sTuples count after random: " + sTuples.size)

    val hLoops = (storesCount / dimensionSize).toInt
    val vLoops = (caCount / dimensionSize).toInt
    logger.info("hLoops: " + hLoops + "vLoops: " + vLoops)

    var regionalJoinObjects = new ListBuffer[RegionalJoin]

    var region = 0
    for (i <- 1 to vLoops) {
      logger.info("i value: " + i)
      val verticalList = sTuples.drop(dimensionSize * (i - 1)).take((dimensionSize * i) - 1)
      logger.info("vertical list size: " + verticalList.size)
      region += 1
      for (j <- 1 to hLoops) {
        logger.info("j value: " + j)
        val horizontalList = tTuples.drop(dimensionSize * (j - 1)).take((dimensionSize * j) - 1)
        logger.info("horizontal list size: " + horizontalList.size)

        val rj = RegionalJoin(region, horizontalList, verticalList)
        logger.info("regional join obj created: " + rj.toString)
        regionalJoinObjects += rj
        region += 1
      }
    }

    //create rdd
    val rddOmg = sc.parallelize(regionalJoinObjects).map(rj => (rj.regionNumber, rj))
    logger.info("rddOmg count : " + rddOmg.count())
    rddOmg.foreach(record => {
      logger.info("region: " + record._1 + ", countT: " + record._2.tTuples.size + "countS: " + record._2.sTuples.size)
    })

    // --------------------------------------
    //Try to save results to file
    try {
      //w/e -> saveAsTextFile(pathToExportResults)
      //      this.logger.info("Saved theta join result tuples to : " + pathToExportResults)
    } catch {
      case e: Exception => this.logger.error("Couldn't save file. Message is : " + e.getMessage)
    }
    logger.info("telos")
    readChar() //Pauses execution to allow for inspection
  }
}
