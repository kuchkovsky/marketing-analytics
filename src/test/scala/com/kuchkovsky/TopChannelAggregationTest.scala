package com.kuchkovsky

import com.kuchkovsky.aggregations.{TopChannelAggregationDataFrame, TopChannelAggregationSparkSQL}
import com.kuchkovsky.model.PurchasesAttribution
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec

class TopChannelAggregationTest extends wordspec.AnyWordSpec with Matchers with SparkTestingBase {
    private val purchasesAttribution = Utils
        .readDatasetFromCsv[PurchasesAttribution](Schema.purchasesAttribution, s"$resourcesPath/purchases_attribution.csv")

    private val expected = Array(
        Row("Google Ads", "cmp1", 2L),
        Row("Yandex Ads", "cmp10", 1L),
        Row("Yandex Ads", "cmp11", 1L),
        Row("Google Ads", "cmp12", 1L),
        Row("Yandex Ads", "cmp2", 3L),
        Row("Google Ads", "cmp3", 1L),
        Row("Yandex Ads", "cmp4", 1L),
        Row("Yandex Ads", "cmp5", 1L),
        Row("Google Ads", "cmp6", 1L),
        Row("Google Ads", "cmp7", 1L),
        Row("Google Ads", "cmp8", 1L),
        Row("Yandex Ads", "cmp9", 1L)
    )

    "TopChannelAggregationSparkSQL" should {
        "properly aggregate the data" in {
            val topChannel = new TopChannelAggregationSparkSQL().aggregate(purchasesAttribution)

            topChannel.collect shouldEqual expected
        }
    }

    "TopChannelAggregationDataFrame" should {
        "properly aggregate the data" in {
            val topChannel = new TopChannelAggregationDataFrame().aggregate(purchasesAttribution)

            topChannel.collect shouldEqual expected
        }
    }
}
