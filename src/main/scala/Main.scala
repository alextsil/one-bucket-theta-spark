import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    //init
    val conf = new SparkConf()
      .setAppName("one-bucket-theta-spark")

    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder().getOrCreate

    //data loading --
    val storeDF = spark.read.parquet("/media/alextsil/sparkPerfData/tpcds/store/part-r-00000-247e1f26-c381-44d9-b91a-3698745e51b8.snappy.parquet")
    val caDF = spark.read.parquet("/media/alextsil/sparkPerfData/tpcds/customer_address/part-r-00000-7906df48-30c1-46ee-921a-a56ef3ba34b4.snappy.parquet")

    //DF to RDD
    val storesRdd = storeDF.rdd
    val caRdd = caDF.rdd

    //Map using zip as key
    val mappedStores = storesRdd.map(storeRow => (storeRow.get(25), storeRow))
    val mappedCa = caRdd.map(caRow => (caRow.get(9), caRow))

    val partitionedCa = mappedCa.partitionBy(new RandomPartitioner(args(0).toInt))
    partitionedCa.persist.collect
    partitionedCa.join(mappedStores).collect()

    readChar()
  }
}
