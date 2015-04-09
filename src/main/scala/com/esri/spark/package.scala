package com.esri

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  */
package object spark {

  implicit class WeightedStatStats(rdd: RDD[WeightedValue]) extends Logging with Serializable {

    def stats(): WeightedStatCounter = {
      rdd.mapPartitions(weightedValues => Iterator(WeightedStatCounter(weightedValues))).reduce((a, b) => a.merge(b))
    }

    def mean(): Double = stats().mean

    def variance(): Double = stats().variance

    def stdev(): Double = stats().stdev

  }

  implicit class DataFrameStats(dataFrame: DataFrame) extends Logging with Serializable {
    def stats(valueFieldName: String, weightFieldName: String): WeightedStatCounter = {
      val schema = dataFrame.schema
      val valueIndex = schema.fields.indexOf(schema(valueFieldName))
      val weightIndex = schema.fields.indexOf(schema(weightFieldName))
      dataFrame.rdd.map(row => WeightedValue(row.getDouble(valueIndex), row.getDouble(weightIndex))).stats()
    }
  }

}
