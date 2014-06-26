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

package org.apache.spark.shuffle

import org.apache.spark.storage.{BlockId, FileSegment, ShuffleBlockId}
import org.apache.spark.{SparkConf, Logging}
import java.nio.ByteBuffer
import org.apache.spark.network.netty.PathResolver

private[spark]
trait ShuffleBlockManager extends PathResolver {
  type ShuffleId = Int

  /**
   * Returns the physical file segment in which the given BlockId is located.
   * This function should only be called if shuffle file consolidation is enabled, as it is
   * an error condition if we don't find the expected block.
   */
  def getBlockLocation(id: ShuffleBlockId): FileSegment

  def getBytes(blockId: ShuffleBlockId): Option[ByteBuffer]

  def stop(): Unit
}

