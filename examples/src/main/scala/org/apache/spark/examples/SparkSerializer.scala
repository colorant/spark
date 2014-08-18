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
import java.io._
import java.nio.channels.FileChannel.MapMode
import java.nio.ByteBuffer
import org.apache.spark.util.ByteBufferInputStream


class MyRegistrator4SparkSerializer extends KryoRegistrator {
  override def registerClasses(k: Kryo) {
    k.register(classOf[(String, Long)])
  }
}

/** Word Count */
object SparkSerializer {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage : SparkWordCount <master> <caseNum> <partition number> <Item Number> <file path>")
      System.err.println("case 0 : Java Serializer write")
      System.err.println("case 1 : Kryo Serializer write")
      System.err.println("case 2 : Java Serializer read")
      System.err.println("case 3 : Kryo Serializer read")

      System.exit(1)
    }

    var conf = new SparkConf(false)

    val caseNum = args(1).toInt
    val parNum = args(2).toInt
    val itemNum = args(3).toLong

    var file: String = null
    if (args.length == 5) {
      file = args(4).toString
    }


    caseNum match {
      case 0 =>
        conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

      case 1 =>
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.kryo.registrator", classOf[MyRegistrator4SparkSerializer].getName)

      case 2 =>
        conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

      case 3 =>
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.kryo.registrator", classOf[MyRegistrator4SparkSerializer].getName)

      case _ =>

    }


    val sc = new SparkContext(args(0), "SparkSerializer", conf)
    val s = "A String for Serialize"
    val s2 = (s, 1L)

    if (caseNum == 0 || caseNum ==1) {
      val resultRDD = sc.makeRDD((1 to parNum), parNum).map{ x =>
        //val f: FileOutputStream = new FileOutputStream("/dev/null")
        var dataCount: Long = 0L
        val ser = SparkEnv.get.serializer.newInstance()

        var num:Long = 0L

        if (file != null) {
          val f = new File(file + x.toString)

          val out: OutputStream = {
            new BufferedOutputStream(new FileOutputStream(f), 1024 * 100)
          }

          val serOut = ser.serializeStream(out)

          while(num < itemNum) {
            num += 1
            serOut.writeObject((s, num))
          }

          serOut.flush()
          serOut.close()
          dataCount = f.length()

        } else {
          while(num < itemNum) {
            num += 1
            val data = ser.serialize((s, num))
            dataCount += data.limit
          }
        }
        dataCount
      }
      resultRDD.collect().foreach(x => println("ser, size of data = " + x))
    }

    if (caseNum == 2 || caseNum == 3) {
      val resultRDD = sc.makeRDD((1 to parNum), parNum).map{ x =>
      //val f: FileOutputStream = new FileOutputStream("/dev/null")
        var dataCount: Long = 0L
        val ser = SparkEnv.get.serializer.newInstance()

        if (file != null) {
          val f = new File(file + x.toString)

          val channel = new RandomAccessFile(new File(file + x.toString), "r").getChannel
          val bytes: ByteBuffer= channel.map(MapMode.READ_ONLY, 0, f.length())

          val in: InputStream = {
            new ByteBufferInputStream(bytes, true)
          }

          val serIn = ser.deserializeStream(in)
          val p = serIn.asIterator.asInstanceOf[Iterator[(String, Long)]]
          while(p.hasNext) {
            p.next()
          }
/*
          var finished = false
          while(!finished) {
            try {
              //serIn.readObject[(String, Long)]()
              serIn.readObject[(String, Long)]()
            } catch {
              case eof: EOFException =>
                finished = true
            }

          }
  */
          serIn.close()
          dataCount = f.length()

        } else {
          var num:Long = 0L
          val data = ser.serialize((s, Long))

          while(num < itemNum) {
            data.rewind()
            num += 1
            ser.deserialize[(String, Long)](data)
            dataCount += data.limit
          }
        }
        dataCount
      }

      resultRDD.collect().foreach(x => println("deser, size of data= " + x))
    }

    sc.stop()
  }
}
