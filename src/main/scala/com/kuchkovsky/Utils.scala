package com.kuchkovsky

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

object Utils {
    def readDatasetFromCsv[T <: Product: TypeTag](schema: StructType, path: String)
                                                 (implicit spark: SparkSession): Dataset[T] = {
        import spark.implicits._

        spark.read
            .option("header", "true")
            .schema(schema)
            .csv(path)
            .as[T]
    }

    def writeDataFrameToCsv(df: DataFrame, path: String): Unit =
        df.coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .option("header", "true")
            .csv(path)
}
