/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import java.util.Random

import breeze.linalg.{Vector, DenseVector, squaredDistance}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * K-means clustering.
 */
object SparkKMeans2 {

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ').map(_.toDouble))
  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def cacheRDD[U: ClassTag] (rdd: RDD[U], level: Int): RDD[U] = {

    val cl = level match {
      case 0 =>
        StorageLevel.NONE
      case 1 =>
        StorageLevel.MEMORY_ONLY
      case 2 =>
        StorageLevel.DISK_ONLY
      case 3 =>
        StorageLevel.MEMORY_AND_DISK
    }

    rdd.persist(cl)

  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: SparkKMeans2 <dir> <k> <iteration> <cache level>")
      System.err.println("cache level : 0 no, 1 memory only, 2 disk only, 3 memory + disk")
      System.exit(1)
    }

    val dir = args(0).toString
    val K = args(1).toInt
    val iteration = args(2).toInt
    val cacheLevel = args(3).toInt

    val sparkConf = new SparkConf().setAppName("SparkKMeans")
    val sc = new SparkContext(sparkConf)

    val loadRDD = sc.objectFile[(Double, Double, Double)](dir)
    val points = loadRDD.map{ p =>
      val arr = new Array[Double](3)
      arr(0) = p._1
      arr(1) = p._2
      arr(2) = p._3
      Vector(arr)
    }

    val data = cacheRDD(points, cacheLevel)

    val kPoints = data.takeSample(withReplacement = false, K, 42).toArray
    var tempDist = 1.0

    var i = 0

    while(i < iteration) {
      i = i + 1

      val closest = data.map (p => (closestPoint(p, kPoints), (p, 1)))

      val pointStats = closest.reduceByKey{case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}

      val newPoints = pointStats.map {pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()

      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      println("Finished iteration (delta = " + tempDist + ")")
    }

    println("Final centers:")
    kPoints.foreach(println)
    sc.stop()
  }
}
