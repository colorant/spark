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
import java.util.Random
import scala.reflect.ClassTag
import org.apache.spark.rdd.{DataGenRDD, DataGenRDDPartition, DataGenerator, RDD}
import breeze.linalg.DenseVector

/*
class RandomRDDIterator(seed: Int, size: Long) extends Iterator[(Double, Double, Double)] {
  var index: Long = 0
  val ranGen = new Random(seed)

  def hasNext: Boolean = (index < size)

  def next(): (Double, Double, Double) = {
    index = index + 1
    (ranGen.nextDouble, ranGen.nextDouble, ranGen.nextDouble)
  }

}

class RandomRDDPartition(val i: Int) extends Partition with Serializable {
  override val index: Int = i
}

class RandomRDD(
    @transient sc: SparkContext,
    partitionNum: Int,
    PartitionSize: Long)
  extends RDD[(Double, Double, Double)](sc, Nil) with Logging  {

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](partitionNum)
    for (i <- 0 until partitionNum) {
      array(i) = new RandomRDDPartition(i)
    }
    array
  }

  override def compute(split: Partition, context: TaskContext) = {
    new RandomRDDIterator(split.index, PartitionSize)
  }

}
*/

class KMeansDataGen(size: Long) extends DataGenerator[(Double, Double, Double)] {

  var ranGen: Random = _

  override def init(split: DataGenRDDPartition) = {
    ranGen = new Random(split.index)
  }

  var index: Long = 0

  override def hasNext: Boolean = (index < size)

  override def next(): (Double, Double, Double) = {
    index = index + 1
    (ranGen.nextDouble, ranGen.nextDouble, ranGen.nextDouble)
  }

}

object GenKMeans {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage : GenKMeans <master> <dir> <partition number> <partition size>")
      System.exit(1)
    }

    val sc = new SparkContext(args(0), "GenKMeans",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)

    val dir = args(1).toString
    val partitionNum = args(2).toInt
    val parSize = args(3).toInt

/*
    val genRdd = sc.makeRDD(1 to partitionNum, partitionNum)

    val pointsRDD = genRdd.flatMap{ p =>
      val ranGen = new Random(p)
      var points = new Array[(Double, Double, Double)](parSize)
      for (i <- 0 until parSize) {
        points(i) = (ranGen.nextDouble, ranGen.nextDouble, ranGen.nextDouble)
      }
      points
    }
*/

    //val pointsRDD = new RandomRDD(sc, partitionNum, parSize)

    val generator = new KMeansDataGen(parSize)
    val pointsRDD = new DataGenRDD(sc, generator, partitionNum)

    // pointsRDD.saveAsTextFile(dir)
    pointsRDD.saveAsObjectFile(dir)

    val loadRDD = sc.objectFile[(Double, Double, Double)](dir)
    val data = loadRDD.map{ p =>
      val arr = new Array[Double](3)
      arr(0) = p._1
      arr(1) = p._2
      arr(2) = p._3
      DenseVector(arr)
    }

    /*
    // print the result vector
    val result = data.collect()
    for (item <- result) {
      System.out.println(item)
    }
    */

    val count = data.count()
    System.out.println("total vector count = " + count)

    sc.stop()
  }
}
