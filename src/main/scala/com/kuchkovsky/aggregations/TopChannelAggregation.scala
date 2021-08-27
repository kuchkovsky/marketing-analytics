package com.kuchkovsky.aggregations

import com.kuchkovsky.model.PurchasesAttribution
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

trait TopChannelAggregation {
    def aggregate(purchasesAttribution: Dataset[PurchasesAttribution])(implicit spark: SparkSession): DataFrame
}

class TopChannelAggregationSparkSQL extends TopChannelAggregation {
    override def aggregate(purchasesAttribution: Dataset[PurchasesAttribution])
                          (implicit spark: SparkSession): DataFrame = {
        purchasesAttribution.createOrReplaceTempView("p_attr")

        spark.sql(
            """SELECT FIRST(channelId) AS channelId, campaignId, FIRST(sessionCount) AS sessionCount FROM (
              |SELECT channelId, campaignId, COUNT(sessionId) AS sessionCount FROM p_attr
              |GROUP BY channelId, campaignId
              |ORDER BY campaignId ASC, sessionCount DESC)
              |GROUP BY campaignId
              |ORDER BY campaignId
              |""".stripMargin
        )
    }
}

class TopChannelAggregationDataFrame extends TopChannelAggregation {
    override def aggregate(purchasesAttribution: Dataset[PurchasesAttribution])
                          (implicit spark: SparkSession): DataFrame = {
        val subQuery = purchasesAttribution.groupBy("channelId", "campaignId")
            .agg(count("sessionId").as("sessionCount"))
            .orderBy(asc("campaignId"), desc("sessionCount"))

        subQuery.groupBy("campaignId")
            .agg(first("channelId").as("channelId"),
                first("sessionCount").as("sessionCount"))
            .orderBy("campaignId")
            .select("channelId", "campaignId", "sessionCount")
    }
}
