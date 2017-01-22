import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
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
      val verticalList = sTuples.drop(dimensionSize * (i - 1)).take((dimensionSize * i)).toVector
      logger.info("vertical list size: " + verticalList.size)
      for (j <- 1 to hLoops) {
        region += 1
        logger.info("j value: " + j)
        val horizontalList = tTuples.drop(dimensionSize * (j - 1)).take((dimensionSize * j)).toVector
        logger.info("horizontal list size: " + horizontalList.size)

        val rj = RegionalJoin(region, horizontalList, verticalList)
        logger.info("regional join obj created: " + rj.toString)
        regionalJoinObjects += rj
      }
    }
    logger.info("reg join list size : " + regionalJoinObjects.size)
    logger.info("all reg join objects -> start")
    regionalJoinObjects.foreach(rjo => logger.info(rjo.toString))
    logger.info("all reg join objects -> end")

    //create rdd
    val rddOmg = sc.parallelize(regionalJoinObjects).map(rj => (rj.regionNumber, rj)).partitionBy(new MirrorPartitioner(partitionCount))
    logger.info("rddOmg count : " + rddOmg.count())

    rddOmg.map(r => this.thetaJoin(r._2.tTuples, r._2.sTuples)).saveAsTextFile(pathToExportResults)

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

  def thetaJoin(t: Seq[Row], s: Seq[Row]): Integer = {
    //    var thetaOut = new ListBuffer[Row]
    var thetaOut = 0
    for (i <- t.indices) {
      for (j <- s.indices) {
        //== operator should handle null values
        if ((t(i).getAs("s_zip") == s(j).getAs("ca_zip"))) {
          //add to out
          thetaOut += 1
        }
      }
    }
    thetaOut
  }

}
