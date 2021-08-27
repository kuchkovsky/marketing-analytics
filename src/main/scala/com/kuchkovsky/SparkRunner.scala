package com.kuchkovsky

import com.kuchkovsky.aggregations._
import com.kuchkovsky.model.{ClickStream, Purchases}
import org.apache.spark.sql.SparkSession

object SparkRunner {

    def main(args: Array[String]): Unit = {
        val consoleParameters = new ConsoleParameters(args)

        implicit val spark: SparkSession = SparkSession
            .builder
            .appName("Marketing Analytics")
            .master("local[*]")
            .getOrCreate

        val clickStream = Utils
            .readDatasetFromCsv[ClickStream](Schema.clickStream, consoleParameters.clickStreamPath())
            .cache

        val purchases = Utils
            .readDatasetFromCsv[Purchases](Schema.purchases, consoleParameters.purchasesPath())

        val purchasesAttributionAggregation = new PurchasesAttributionSparkSQL
        val purchasesAttribution = purchasesAttributionAggregation.aggregate(clickStream, purchases)
        Utils.writeDataFrameToCsv(purchasesAttribution.toDF, "output/purchases_attribution")

        val top10CampaignsAggregation = consoleParameters.top10Campaigns() match {
            case "SPARK_SQL" => new Top10CampaignsAggregationSparkSQL
            case "DATAFRAME" => new Top10CampaignsAggregationDataFrame
        }
        val top10Campaigns = top10CampaignsAggregation.aggregate(purchasesAttribution)
        Utils.writeDataFrameToCsv(top10Campaigns, "output/top10_campaigns")

        val topChannelAggregation = consoleParameters.topChannel() match {
            case "SPARK_SQL" => new TopChannelAggregationSparkSQL
            case "DATAFRAME" => new TopChannelAggregationDataFrame
        }
        val topChannel = topChannelAggregation.aggregate(purchasesAttribution)
        Utils.writeDataFrameToCsv(topChannel, "output/top_channel")
    }

}
