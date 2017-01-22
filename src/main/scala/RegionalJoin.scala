import org.apache.spark.sql.Row

case class RegionalJoin(regionNumber: Int, tTuples: List[Row], sTuples: List[Row]) {


  override def toString: String = "region number:" + regionNumber + ", tTuplesCount:" + tTuples.size +
    ", sTuplesCount:" + sTuples.size

  //Todo: ask: xreiazetai na override thn equals kai hashcode?
}
