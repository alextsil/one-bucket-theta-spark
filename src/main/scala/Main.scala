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
      val verticalList = sTuples.drop(dimensionSize * (i - 1)).take((dimensionSize)).toVector
      logger.info("vertical list size: " + verticalList.size)
      for (j <- 1 to hLoops) {
        region += 1
        logger.info("j value: " + j)
        val horizontalList = tTuples.drop(dimensionSize * (j - 1)).take((dimensionSize)).toVector
        logger.info("horizontal list size: " + horizontalList.size)

        val rj = RegionalJoin(region,
          horizontalList.filter(r => r.getAs("s_zip") != null),
          verticalList.filter(r => r.getAs("ca_zip") != null))
        logger.info("regional join obj created: " + rj.toString)
        regionalJoinObjects += rj
      }
    }
    logger.info("reg join list size : " + regionalJoinObjects.size)
    logger.info("all reg join objects -> start")
    regionalJoinObjects.foreach(rjo => logger.info(rjo.toString))
    logger.info("all reg join objects -> end")

    //create rdd, map to [regionNumber, RegionalJoin] format and finally partition by region number
    val rdd = sc.parallelize(regionalJoinObjects).map(rj => (rj.regionNumber, rj)).partitionBy(new MirrorPartitioner(partitionCount))
    logger.info("rdd count : " + rdd.count())

    val countRdd = rdd.map(r => this.thetaJoin(r._2.tTuples, r._2.sTuples))
    //Kathe partition bgazei diko tou count output
    //todo: na vrw ena tropo na prosthesw ta counts
    countRdd.saveAsTextFile(pathToExportResults)

    logger.info("One bucket theta finished...")
    readChar() //Pauses execution to allow for inspection
  }

  //Nested loop
  def thetaJoin(t: Seq[Row], s: Seq[Row]): Integer = {
    var thetaOutCount = 0
    for (i <- t.indices) {
      for (j <- s.indices) {
        //The == operator handles null values
        if (!(t(i).getAs("s_zip") == s(j).getAs("ca_zip"))) {
          thetaOutCount += 1
        }
      }
    }
    thetaOutCount
  }

}
