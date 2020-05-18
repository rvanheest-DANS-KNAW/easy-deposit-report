package nl.knaw.dans.easy.managedeposit

object Location extends Enumeration {
  type Location = Value

  // @formatter:off
  val SWORD2              : Value = Value("SWORD2")
  val INGEST_FLOW         : Value = Value("INGEST_FLOW")
  val INGEST_FLOW_ARCHIVED: Value = Value("INGEST_FLOW_ARCHIVED")
  val UNKNOWN             : Value = Value("UNKNOWN")
  // @formatter:on
}
