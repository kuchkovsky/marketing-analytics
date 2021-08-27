package com.kuchkovsky

import org.apache.spark.sql.SparkSession

trait SparkTestingBase {
    implicit val spark: SparkSession = SparkSession
        .builder()
        .appName("Marketing Analytics Tests")
        .master("local[*]")
        .getOrCreate()

    val resourcesPath = "src/test/resources"
}
