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
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, KryoRegistrator}
import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.reflect.ClassTag
import java.io.FileOutputStream


class MyRegistrator4SparkSerializer extends KryoRegistrator {
  override def registerClasses(k: Kryo) {
    k.register(classOf[(String, Int)])
  }
}

/** Word Count */
object SparkSerializer {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage : SparkWordCount <master> <caseNum> <partition number> <Item Number>")
      System.err.println("case 0 : Java Serializer")
      System.err.println("case 1 : Kryo Serializer")

      System.exit(1)
    }

    var conf = new SparkConf(false)

    val caseNum = args(1).toInt
    val parNum = args(2).toInt
    val itemNum = args(3).toLong


    caseNum match {
      case 0 =>
        conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

      case 1 =>
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.kryo.registrator", classOf[MyRegistrator4SparkSerializer].getName)

      case _ =>

    }


    val sc = new SparkContext(args(0), "SparkSerializer", conf)
    val s = "A String for Serialize"

    val resultRDD = sc.makeRDD((1 to parNum), parNum).map{ x =>
      //val f: FileOutputStream = new FileOutputStream("/dev/null")
      var dataCount: Long = 0L
      val ser = SparkEnv.get.serializer.newInstance()

      var num:Long = 0L
      while(num < itemNum) {
        num += 1
        val data = ser.serialize(s)
        dataCount += data.limit
      }
      dataCount
    }

    resultRDD.collect().foreach(x => println("size of data = " + x))

    sc.stop()
  }
}
