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

/** Word Count */
object SparkWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkWordCount <master> <file>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SparkWordCount",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)

    val files = sc.textFile(args(1))
    val words = files.flatMap(_.split(" "))
    val wordsPair = words.map(x => (x, 1))

    val wordsCount = wordsPair.reduceByKey(_ + _)
    val count = wordsCount.count()

    println("Number of words = " + count)
    sc.stop()
  }
}
