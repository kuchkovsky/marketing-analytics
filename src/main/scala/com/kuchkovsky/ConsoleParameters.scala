package com.kuchkovsky

import org.rogach.scallop._

class ConsoleParameters(arguments: Seq[String]) extends ScallopConf(arguments) {
    val clickStreamPath = opt[String](short = 'c', required = true)
    val purchasesPath = opt[String](short = 'p', required = true)
    val top10Campaigns = opt[String](required = true, validate = s => Seq("SPARK_SQL", "DATAFRAME").contains(s))
    val topChannel = opt[String](required = true, validate = s => Seq("SPARK_SQL", "DATAFRAME").contains(s))

    verify()
}
