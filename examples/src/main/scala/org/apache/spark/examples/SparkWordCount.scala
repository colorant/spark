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

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD


class MyRegistrator extends KryoRegistrator {
  override def registerClasses(k: Kryo) {
    k.register(classOf[(String, Int)])
  }
}

/** Word Count */
object SparkWordCount {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage : SparkWordCount <master> <file> <caseNum> <iteration>")
      System.err.println("case 0 : Count directly without reduceByKey")
      System.err.println("case 1 : reduceByKey then count")
      System.err.println("case 2 : repartition to 192 then count")
      System.err.println("case 3 : groupByKey then count")
      System.exit(1)
    }

    val sc = new SparkContext(args(0), "SparkWordCount",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)

    val iteration = args(3).toInt
    /* for text file */

    //val files = sc.textFile(args(1))
    //val words = files.flatMap(_.split(" "))

    /* for Sequence file */
    val files : RDD[(String, String)] = sc.sequenceFile(args(1))
    val words = files.flatMap{ case (x,y) => y.split(" ")}


    val wordsPair = words.map(x => (x, 1))

    var count = 0L

    args(2) match {
      case "0" =>

        for(i <- 1 to iteration) {
          println("count iteration: %d start", i)
          count = wordsPair.count()
          println("count iteration: %d, count = %d", i, count)
        }

      case "1" =>
        val wordsCount = wordsPair.reduceByKey(_ + _)
        count = wordsCount.count()

      case "2" =>
        val wordsRep = wordsPair.repartition(192)
        count = wordsRep.count()

      case "3" =>
        val wordsGroup = wordsPair.groupByKey()
        count = wordsGroup.count()

      case _ => println("wrong case number")
        sc.stop()
        System.exit(2)

    }


    println("Number of words = " + count)
    sc.stop()
  }
}
