import org.apache.spark.sql.Row

case class RegionalJoin(regionNumber: Int, tTuples: List[Row], sTuples: List[Row]) {


  //  override def toString: String = "region number:" + regionNumber + ", tTuplesCount:" + tTuples.count() +
  //    ", sTuplesCount:" + sTuples.count()

  //Todo: ask: xreiazetai na override thn equals kai hashcode?
}
