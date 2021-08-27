package com.kuchkovsky.aggregations

import com.kuchkovsky.model.{ClickStream, Purchases, PurchasesAttribution}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import spray.json._

trait PurchasesAttributionAggregation {
    def aggregate(clickStream: Dataset[ClickStream], purchases: Dataset[Purchases])
                 (implicit spark: SparkSession): Dataset[PurchasesAttribution]
}

class PurchasesAttributionSparkSQL extends PurchasesAttributionAggregation {

    private def extractField(fieldName: String): UserDefinedFunction = {
        val parseAttributes: String => String = s =>
            if (s == null) null
            else s.replaceAll(";", ",").parseJson.asJsObject.getFields(fieldName) match {
                case Seq(JsString(field)) => field
            }

        udf(parseAttributes)
    }

    private val campaignIdUDF: UserDefinedFunction = extractField("campaign_id")

    private val channelIdUDF: UserDefinedFunction = extractField("channel_id")

    private val purchaseIdUDF: UserDefinedFunction = extractField("purchase_id")

    override def aggregate(clickStream: Dataset[ClickStream], purchases: Dataset[Purchases])
                          (implicit spark: SparkSession): Dataset[PurchasesAttribution] = {
        import spark.implicits._

        val appOpen = clickStream.filter(col("eventType") === "app_open").as("appOpen")

        val appClose = clickStream.filter(col("eventType") === "app_close").as("appClose")

        val purchase = clickStream.filter(col("eventType") === "purchase").as("purchase")

        val appOpenAndClose = appOpen.join(appClose, "userId")
            .filter(col("appOpen.eventTime") lt col("appClose.eventTime"))
            .groupBy("appOpen.userId","appOpen.eventId", "appOpen.eventTime", "appOpen.attributes")
            .agg(min("appClose.eventTime").as("closeEventTime"))
            .as("appOpenClose")

        val joinedClickStream = appOpenAndClose.join(purchase, "userId")
            .filter((col("appOpenClose.eventTime") lt col("purchase.eventTime"))
                and (col("appOpenClose.closeEventTime") gt col("purchase.eventTime")))
            .withColumn("purchaseId", purchaseIdUDF(col("purchase.attributes")))

        joinedClickStream.join(purchases, "purchaseId")
            .select(
                col("purchaseId"),
                col("purchaseTime"),
                col("billingCost"),
                col("isConfirmed"),
                monotonically_increasing_id().as("sessionId"),
                campaignIdUDF(col("appOpenClose.attributes")).as("campaignId"),
                channelIdUDF(col("appOpenClose.attributes")).as("channelId")
            )
            .as[PurchasesAttribution]
    }
}
