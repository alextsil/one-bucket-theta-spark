import org.apache.spark.Partitioner

import scala.util.Random

class RandomPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    return Random.nextInt(this.numParts)
  }

  override def equals(other: Any): Boolean = other match {
    case rp: RandomPartitioner =>
      rp.numPartitions == numPartitions
    case _ =>
      false
  }

}
