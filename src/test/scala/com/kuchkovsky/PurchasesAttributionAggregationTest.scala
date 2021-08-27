package com.kuchkovsky

import java.sql.Timestamp

import com.kuchkovsky.aggregations.PurchasesAttributionSparkSQL
import com.kuchkovsky.model.{ClickStream, Purchases, PurchasesAttribution}
import org.scalatest._
import matchers.should._

class PurchasesAttributionAggregationTest extends wordspec.AnyWordSpec with Matchers with SparkTestingBase {

    "PurchasesAttributionAggregationSparkSQL" should {
        "properly aggregate the data" in {
            val clickStream = Utils
                .readDatasetFromCsv[ClickStream](Schema.clickStream, s"$resourcesPath/mobile-app-clickstream.csv")

            val purchases = Utils
                .readDatasetFromCsv[Purchases](Schema.purchases, s"$resourcesPath/purchases.csv")

            val purchasesAttributionAggregation = new PurchasesAttributionSparkSQL()
            val purchasesAttribution = purchasesAttributionAggregation.aggregate(clickStream, purchases)

            val expected = Array(
                PurchasesAttribution("p2", Timestamp.valueOf("2019-01-01 00:03:10"), 200.0D, true, 128849018880L, "cmp1", "Yandex Ads"),
                PurchasesAttribution("p1", Timestamp.valueOf("2019-01-01 00:01:05"), 100.5D, true, 257698037760L, "cmp1", "Google Ads"),
                PurchasesAttribution("p6", Timestamp.valueOf("2019-01-02 13:03:00"), 99.0D, false, 506806140928L, "cmp2", "Yandex Ads"),
                PurchasesAttribution("p3", Timestamp.valueOf("2019-01-01 01:12:15"), 300.0D, false, 712964571136L, "cmp1", "Google Ads"),
                PurchasesAttribution("p5", Timestamp.valueOf("2019-01-01 02:15:05"), 75.0D, true, 1236950581248L, "cmp2", "Yandex Ads"),
                PurchasesAttribution("p4", Timestamp.valueOf("2019-01-01 02:13:05"), 50.2D, true, 1236950581249L, "cmp2", "Yandex Ads"),
            )

            purchasesAttribution.collect shouldBe expected
        }
    }
}
