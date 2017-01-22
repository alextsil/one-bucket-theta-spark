import org.apache.spark.Partitioner

class MirrorPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val keyRet = key.asInstanceOf[Int] - 1 //key-1 giati ta partitions einai 0 based
    keyRet
  }

  override def equals(other: Any): Boolean = other match {
    case rp: MirrorPartitioner =>
      rp.numPartitions == numPartitions
    case _ =>
      false
  }

}