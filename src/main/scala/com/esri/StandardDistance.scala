package com.esri

import java.io.PrintWriter

import com.esri.spark._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  */
object StandardDistance extends App with Logging {

  val (inputPath, outputPath) = args.length match {
    case 2 => (args(0), args(1))
    case _ => ("data/points.csv", "/tmp/wkt.txt")
  }
  val conf = new SparkConf()
    .setAppName(StandardDistance.getClass.getSimpleName)
    .setMaster("local[*]")
    .set("spark.driver.memory", "16g")
    .set("spark.executor.memory", "16g")
    .set("spark.serializer", classOf[KryoSerializer].getName)
    .registerKryoClasses(Array(
    classOf[WeightedValue],
    classOf[WeightedStatCounter]
  ))

  val sc = new SparkContext(conf)
  try {
    val sqlContext = new SQLContext(sc)

    val rows = sc.textFile(inputPath)
      .flatMap(line => {
      try {
        val splits = line.split(',')
        val x = splits(1).toDouble
        val y = splits(2).toDouble
        val w = splits(3).toDouble
        Some(Row(x, y, w))
      }
      catch {
        case t: Throwable => None
      }
    })

    val schema = StructType(
      Seq(
        StructField("x", DoubleType),
        StructField("y", DoubleType),
        StructField("w", DoubleType)
      )
    )

    val df = sqlContext.createDataFrame(rows, schema)

    val xStats = df.stats("x", "w")
    val yStats = df.stats("y", "w")

    val xCenter = xStats.mean
    val yCenter = yStats.mean
    val stdDist = math.sqrt(xStats.variance + yStats.variance)

    val wkt = (for (i <- 0 to 360) yield {
      val rad = i.toDouble.toRadians
      val x = xCenter + stdDist * math.cos(rad)
      val y = yCenter + stdDist * math.sin(rad)
      "%.2f %.2f".format(x, y)
    }).mkString("POLYGON ((", ",", "))")

    val pw = new PrintWriter(outputPath)
    try {
      pw.print(wkt)
      pw.print('\t')
      pw.print(xCenter)
      pw.print('\t')
      pw.print(yCenter)
      pw.print('\t')
      pw.print(stdDist)
      pw.println()
    }
    finally {
      pw.close()
    }

  } finally {
    sc.stop()
  }
}
