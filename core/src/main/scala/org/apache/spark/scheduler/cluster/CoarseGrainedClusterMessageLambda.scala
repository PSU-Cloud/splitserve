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

package org.apache.spark.scheduler.cluster

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.ExecutorLossReason
import org.apache.spark.util.SerializableBuffer

private[spark] sealed trait CoarseGrainedClusterMessageLambda extends Serializable

private[spark] object CoarseGrainedClusterMessagesLambda {

  case object RetrieveSparkAppConfig extends CoarseGrainedClusterMessageLambda

  case class SparkAppConfig(
      sparkProperties: Seq[(String, String)],
      ioEncryptionKey: Option[Array[Byte]])
    extends CoarseGrainedClusterMessageLambda

  case object RetrieveLastAllocatedExecutorId extends CoarseGrainedClusterMessageLambda

  // Driver to executors
  case class LaunchTask(data: SerializableBuffer) extends CoarseGrainedClusterMessageLambda

  case class KillTask(taskId: Long, executor: String, interruptThread: Boolean)
    extends CoarseGrainedClusterMessageLambda

  sealed trait RegisterExecutorResponse

  case object RegisteredExecutor extends CoarseGrainedClusterMessageLambda with RegisterExecutorResponse

  case class RegisterExecutorFailed(message: String) extends CoarseGrainedClusterMessageLambda
    with RegisterExecutorResponse

  // Executors to driver
  case class RegisterExecutor(
      executorId: String,
      executorRef: RpcEndpointRef,
      hostname: String,
      cores: Int,
      logUrls: Map[String, String])
    extends CoarseGrainedClusterMessageLambda

  case class LambdaDetails(
      executorId: String,
      awsRequestId: String,
      logGroupName: String,
      logStreamName: String)
    extends CoarseGrainedClusterMessageLambda

  case class StatusUpdate(executorId: String, taskId: Long, state: TaskState,
    data: SerializableBuffer) extends CoarseGrainedClusterMessageLambda

  object StatusUpdate {
    /** Alternate factory method that takes a ByteBuffer directly for the data field */
    def apply(executorId: String, taskId: Long, state: TaskState, data: ByteBuffer)
      : StatusUpdate = {
      StatusUpdate(executorId, taskId, state, new SerializableBuffer(data))
    }
  }

  // Internal messages in driver
  case object ReviveOffers extends CoarseGrainedClusterMessageLambda

  case object StopDriver extends CoarseGrainedClusterMessageLambda

  case object StopExecutor extends CoarseGrainedClusterMessageLambda

  case object StopExecutors extends CoarseGrainedClusterMessageLambda

  case class RemoveExecutor(executorId: String, reason: ExecutorLossReason)
    extends CoarseGrainedClusterMessageLambda

  case class SetupDriver(driver: RpcEndpointRef) extends CoarseGrainedClusterMessageLambda

  // Exchanged between the driver and the AM in Yarn client mode
  case class AddWebUIFilter(
      filterName: String, filterParams: Map[String, String], proxyBase: String)
    extends CoarseGrainedClusterMessageLambda

  // Messages exchanged between the driver and the cluster manager for executor allocation
  // In Yarn mode, these are exchanged between the driver and the AM

  case class RegisterClusterManager(am: RpcEndpointRef) extends CoarseGrainedClusterMessageLambda

  // Request executors by specifying the new total number of executors desired
  // This includes executors already pending or running
  case class RequestExecutors(
      requestedTotal: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int])
    extends CoarseGrainedClusterMessageLambda

  // Check if an executor was force-killed but for a reason unrelated to the running tasks.
  // This could be the case if the executor is preempted, for instance.
  case class GetExecutorLossReason(executorId: String) extends CoarseGrainedClusterMessageLambda

  case class KillExecutors(executorIds: Seq[String]) extends CoarseGrainedClusterMessageLambda

  // Used internally by executors to shut themselves down.
  case object Shutdown extends CoarseGrainedClusterMessageLambda

}
