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
import org.apache.spark.serializer
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}
import com.esotericsoftware.kryo.Kryo
import java.io.{BufferedOutputStream, FileOutputStream, OutputStream}


class Registrator4Serializer extends KryoRegistrator {
  override def registerClasses(k: Kryo) {
    k.register(classOf[(String, Int)])
  }
}

object SerializerTest {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage : SerializerTest <Master> <datasize> <outputfile> ")
      System.exit(1)
    }

    val sc = new SparkContext(args(0), "SerializerTest",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)


    val datasize =  args(1).toInt
    val dataset = (0 until datasize).map( i => ("asmallstring", i))

    val out: OutputStream = {
        new BufferedOutputStream(new FileOutputStream(args(2)), 1024 * 100)
      }

    val ser = SparkEnv.get.serializer.newInstance()
    val serOut = ser.serializeStream(out)

    dataset.foreach( value =>
      serOut.writeObject(value)
    )
    serOut.flush()
    serOut.close()

    sc.stop()

  }
}
