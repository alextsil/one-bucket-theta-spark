import org.apache.spark.sql.Row
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.mutable.ListBuffer

//run panw sto spark-shell
val storeDF = spark.read.parquet("/media/alextsil/sparkPerfData/tpcds/store/part-r-00000-6f481877-b677-415c-b8c7-1ac8d85573f1.snappy.parquet")

val storeRowList = storeDF.collect().to[ListBuffer]
    var newList: ListBuffer[Row] = new ListBuffer[Row]
    for (i <- 1 to 1366) {
      newList ++= storeRowList
    }
    newList.remove(500, 8) //remove 8 apo to index 500(stin tyxh)
    for (i <- newList.indices) {
     val oldRow = newList(i)
      val newRow = Row(i+1, oldRow(1), oldRow(2), oldRow(3), oldRow(4), oldRow(5), oldRow(6), oldRow(7), oldRow(8),
        oldRow(9), oldRow(10), oldRow(11), oldRow(12), oldRow(13), oldRow(14), oldRow(15), oldRow(16), oldRow(17),
        oldRow(18), oldRow(19), oldRow(20), oldRow(21), oldRow(22), oldRow(23), oldRow(24), oldRow(25), oldRow(26),
        oldRow(27), oldRow(28))
      newList.update(i, newRow)
    }
    val finalStoreDF = spark.createDataFrame(newList, storeDF.schema)
    finalStoreDF.write.parquet("/media/alextsil/sparkPerfData/tpcds/store/16kstores.parquet")

