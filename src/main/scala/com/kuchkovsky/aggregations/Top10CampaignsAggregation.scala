package com.kuchkovsky.aggregations

import com.kuchkovsky.model.PurchasesAttribution
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

trait Top10CampaignsAggregation {
    def aggregate(purchasesAttribution: Dataset[PurchasesAttribution])(implicit spark: SparkSession): DataFrame
}

class Top10CampaignsAggregationSparkSQL extends Top10CampaignsAggregation {
    override def aggregate(purchasesAttribution: Dataset[PurchasesAttribution])
                          (implicit spark: SparkSession): DataFrame = {
        purchasesAttribution.createOrReplaceTempView("p_attr")

        spark.sql(
            """SELECT campaignId, SUM(billingCost) AS billingCostTotal FROM p_attr
              |WHERE isConfirmed IS TRUE
              |GROUP BY campaignId
              |ORDER BY billingCostTotal DESC
              |LIMIT 10
              |""".stripMargin)
    }
}

class Top10CampaignsAggregationDataFrame extends Top10CampaignsAggregation {
    override def aggregate(purchasesAttribution: Dataset[PurchasesAttribution])
                          (implicit spark: SparkSession): DataFrame =
        purchasesAttribution.where(col("isConfirmed"))
            .groupBy("campaignId")
            .agg(sum("billingCost").as("billingCostTotal"))
            .orderBy(desc("billingCostTotal"))
            .limit(10)
}
