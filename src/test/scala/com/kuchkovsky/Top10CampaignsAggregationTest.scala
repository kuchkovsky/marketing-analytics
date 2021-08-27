package com.kuchkovsky

import com.kuchkovsky.aggregations.{Top10CampaignsAggregationDataFrame, Top10CampaignsAggregationSparkSQL}
import com.kuchkovsky.model.PurchasesAttribution
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec

class Top10CampaignsAggregationTest extends wordspec.AnyWordSpec with Matchers with SparkTestingBase {
    private val purchasesAttribution = Utils
        .readDatasetFromCsv[PurchasesAttribution](Schema.purchasesAttribution, s"$resourcesPath/purchases_attribution.csv")

    private val expected = Array(
        Row("cmp1", 300.5D),
        Row("cmp2", 125.2D),
        Row("cmp12", 100.0D),
        Row("cmp11", 90.0D),
        Row("cmp10", 80.0D),
        Row("cmp9", 70.0D),
        Row("cmp8", 60.0D),
        Row("cmp7", 50.0D),
        Row("cmp6", 40.0D),
        Row("cmp5", 30.0D),
    )

    "Top10CampaignsAggregationSparkSQLTest" should {
        "properly aggregate the data" in {
            val top10Campaigns = new Top10CampaignsAggregationSparkSQL().aggregate(purchasesAttribution)

            top10Campaigns.collect shouldEqual expected
        }
    }

    "Top10CampaignsAggregationDataFrameTest" should {
        "properly aggregate the data" in {
            val top10Campaigns = new Top10CampaignsAggregationDataFrame().aggregate(purchasesAttribution)

            top10Campaigns.collect shouldEqual expected
        }
    }
}
