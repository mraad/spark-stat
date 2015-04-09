package com.esri.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  */
class StatSuite extends FunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = _

  override def beforeAll() {
    super.beforeAll()
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("StatSuite")
    sc = new SparkContext(conf)
  }

  test("Test Mean and StdDev") {

    val values = Seq(
      WeightedValue(10.0, 1.0),
      WeightedValue(12.0, 2.0),
      WeightedValue(14.0, 3.0),
      WeightedValue(16.0, 2.0)
    )

    val deno = values.map(wv => wv.weight).reduce(_ + _)

    val nume1 = values.map(wv => wv.value * wv.weight).reduce(_ + _)

    val mean = nume1 / deno

    val nume2 = values.map(wv => {
      val delta = wv.value - mean
      wv.weight * delta * delta
    }).reduce(_ + _)

    val stdev = math.sqrt(nume2 / deno)

    val stats = sc.parallelize(values).stats()

    assert(math.abs(stats.mean - mean) < 1E-8)
    assert(math.abs(stats.stdev - stdev) < 1E-8)
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }
}
