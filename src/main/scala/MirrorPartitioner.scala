import org.apache.spark.Partitioner

class MirrorPartitioner(numParts: Int) extends Partitioner {

  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val keyRet = key.asInstanceOf[Int] - 1 //-1 giati partitioner einai 0 based
    println("getPartition called me key: " + keyRet)
    keyRet
  }

  override def equals(other: Any): Boolean = other match {
    case rp: MirrorPartitioner =>
      rp.numPartitions == numPartitions
    case _ =>
      false
  }

}