package com.kuchkovsky

import org.apache.spark.sql.types._

object Schema {
    val clickStream = StructType(Seq(
        StructField("userId", StringType),
        StructField("eventId", StringType),
        StructField("eventTime", TimestampType),
        StructField("eventType", StringType),
        StructField("attributes", StringType)
    ))

    val purchases = StructType(Seq(
        StructField("purchaseId", StringType),
        StructField("purchaseTime", TimestampType),
        StructField("billingCost", DoubleType),
        StructField("isConfirmed", BooleanType)
    ))

    val purchasesAttribution = StructType(Seq(
        StructField("purchaseId", StringType),
        StructField("purchaseTime", TimestampType),
        StructField("billingCost", DoubleType),
        StructField("isConfirmed", BooleanType),
        StructField("sessionId", LongType),
        StructField("campaignId", StringType),
        StructField("channelId", StringType)
    ))
}
