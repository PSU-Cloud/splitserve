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

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.collection.mutable.{HashMap, HashSet}
import scala.concurrent.ExecutionContext.Implicits.global

import org.json4s._
import org.json4s.jackson.Serialization

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.client.{StandaloneAppClient, StandaloneAppClientListener}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.{DYN_ALLOCATION_MAX_EXECUTORS}
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.lambda.AWSLambdaClientBuilder
import com.amazonaws.services.lambda.invoke.{LambdaFunction, LambdaInvokerFactory}
import com.amazonaws.services.lambda.model.InvokeRequest
import com.google.common.util.concurrent.RateLimiter

//AMAN: Classes for Lambda executor

class Request {
    /* We don't need S3 for executor binaries anymore
     * But we are keeping these around in case we 
     * circle back for initial input and final output
     */
  
  var sparkS3Bucket: String = _
  def getSparkS3Bucket: String = sparkS3Bucket
  def setSparkS3Bucket(i: String): Unit = { sparkS3Bucket = i }

  var sparkS3Key: String = _
  def getSparkS3Key: String = sparkS3Key
  def setSparkS3Key(i: String): Unit = { sparkS3Key = i }

  var sparkDriverHostname: String = _
  def getSparkDriverHostname: String = sparkDriverHostname
  def setSparkDriverHostname(i: String): Unit = { sparkDriverHostname = i }

  var sparkDriverPort: String = _
  def getSparkDriverPort: String = sparkDriverPort
  def setSparkDriverPort(i: String): Unit = { sparkDriverPort = i }

  var sparkCommandLine: String = _
  def getSparkCommandLine: String = sparkCommandLine
  def setSparkCommandLine(i: String): Unit = { sparkCommandLine = i }

  override def toString() : String = {
    s"Lambda Request: sparkS3Bucket=$sparkS3Bucket\n" +
      s"\t\tsparkS3Key=$sparkS3Key\n" +
      s"\t\tsparkDriverHostname=$sparkDriverHostname\n" +
      s"\t\tsparkDriverPort=$sparkDriverPort\n" +
      s"\t\tsparkCommandLine=$sparkCommandLine\n"
  }
}

case class LambdaRequestPayload (
  sparkS3Bucket: String,
  sparkS3Key: String,
  sparkDriverHostname: String,
  sparkDriverPort: String,
  sparkCommandLine: String,
  javaPartialCommandLine: String,
  executorPartialCommandLine: String
)

class Response {
  var output: String = _
  def getOutput() : String = output
  def setOutput(o: String) : Unit = { output = o }
  override def toString() : String = {
    s"Lambda Response: output=$output"
  }
}

// AMAN: see if we can make it mutable and not hard coded
trait LambdaExecutorService {
  @LambdaFunction(functionName = "spark-lambda")
  def runExecutor(request: Request) : Response
}


/**
 * A [[SchedulerBackend]] implementation for Spark's standalone cluster manager
 * with AWS Lambda support.
 */

private[spark] class StandaloneSchedulerBackend(
      scheduler: TaskSchedulerImpl,
      sc: SparkContext,
      masters: Array[String])
    extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
    with StandaloneAppClientListener
  with Logging {

  private var client: StandaloneAppClient = null
  private var stopping = false
  private val launcherBackend = new LauncherBackend() {
    override protected def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  @volatile var shutdownCallback: StandaloneSchedulerBackend => Unit = _
  @volatile private var appId: String = _

  private val registrationBarrier = new Semaphore(0)

  private val maxCores = conf.getOption("spark.cores.max").map(_.toInt)
  private val totalExpectedCores = maxCores.getOrElse(0)

  //AMAN: Lambda support


  val lambdaBucket = Option(sc.getConf.get("spark.lambda.s3.bucket"))

  if (!lambdaBucket.isDefined) {
    throw new Exception(s"spark.lambda.s3.bucket should" +	
      s" have a valid S3 bucket name (eg: s3://lambda) having Spark binaries")	
  }

  val lambdaFunctionName = sc.conf.get("spark.lambda.function.name", "spark-lambda")
  val s3SparkVersion = sc.conf.get("spark.lambda.spark.software.version", "LATEST")
  var numExecutorsExpected = 0
  var numExecutorsRegistered = new AtomicInteger(0)
  var executorId = new AtomicInteger(1)
  
  // Mapping from executorId to Thread object which is currently in the Lambda RPC call
  var pendingLambdaRequests = new HashMap[String, Thread]
  // Set of executorIds which are currently alive
  val liveExecutors = new HashSet[String]

  var lambdaContainerMemory: Int = 0
  var lambdaContainerTimeoutSecs: Int = 0

  val clientConfig = new ClientConfiguration()
  clientConfig.setClientExecutionTimeout(720000)
  clientConfig.setConnectionTimeout(720000)
  clientConfig.setRequestTimeout(720000)
  clientConfig.setSocketTimeout(720000)

  // AMAN: path set to /opt since using Lambda layers
  val defaultClasspath = s"/mnt/shuffleDir/SplitServe/jars/*,/mnt/shuffleDir/SplitServe/conf/*"
  val lambdaClasspathStr = sc.conf.get("spark.lambda.classpath", defaultClasspath)
  val lambdaClasspath = lambdaClasspathStr.split(",").map(_.trim).mkString(":")

  val lambdaClient = AWSLambdaClientBuilder
                        .standard()
                        .withClientConfiguration(clientConfig)
                        .build()

  final val lambdaExecutorService: LambdaExecutorService =
    LambdaInvokerFactory.builder()
    .lambdaClient(lambdaClient)
    .build(classOf[LambdaExecutorService])
  logInfo(s"Created LambdaExecutorService: $lambdaExecutorService")

  val maxConcurrentRequests = sc.conf.getInt("spark.lambda.concurrent.requests.max", 100)
  val limiter = RateLimiter.create(maxConcurrentRequests)

    override def start() {
	super.start()
	logInfo("start")

	launcherBackend.connect()

	 // The endpoint for executors to talk to us
	val driverUrl = RpcEndpointAddress(
		sc.conf.get("spark.driver.host"),
		sc.conf.get("spark.driver.port").toInt,
		CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

	val args = Seq(
	      "--driver-url", driverUrl,
	      "--executor-id", "{{EXECUTOR_ID}}",
	      "--hostname", "{{HOSTNAME}}",
	      "--cores", "{{CORES}}",
	      "--app-id", "{{APP_ID}}",
	      "--worker-url", "{{WORKER_URL}}",
              "--executor-type", "VM")

        // logInfo(s"AMAN: Argument constructed : $args")

	val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions")
		.map(Utils.splitCommandString).getOrElse(Seq.empty)
	val classPathEntries = sc.conf.getOption("spark.executor.extraClassPath")
		.map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
	val libraryPathEntries = sc.conf.getOption("spark.executor.extraLibraryPath")
		.map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

	// When testing, expose the parent class path to the child. This is processed by
	// compute-classpath.{cmd,sh} and makes all needed jars available to child processes
	// when the assembly is built with the "*-provided" profiles enabled.
	val testingClassPath =
		if (sys.props.contains("spark.testing")) {
			sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
		} else {
			Nil
		}

	// Start executors with a few necessary configs for registering with the scheduler
	val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
	val javaOpts = sparkJavaOpts ++ extraJavaOpts
	val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
		args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
	val appUIAddress = sc.ui.map(_.appUIAddress).getOrElse("")
	val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt)
		// If we're using dynamic allocation, set our initial executor limit to 0 for now.
		// ExecutorAllocationManager will send the real initial limit to the Master later.
	val initialExecutorLimit =
		if (Utils.isDynamicAllocationEnabled(conf)) {
			Some(0)
		} else {
			None
		}
	val appDesc = new ApplicationDescription(sc.appName, maxCores, sc.executorMemory, command,
	appUIAddress, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor, initialExecutorLimit)
	client = new StandaloneAppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
	client.start()
	launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
	waitForRegistration()
	launcherBackend.setState(SparkAppHandle.State.RUNNING)
	
	val request = new com.amazonaws.services.lambda.model.GetFunctionRequest
	request.setFunctionName(lambdaFunctionName)
	val result = lambdaClient.getFunction(request)
	// logInfo(s"LAMBDA: 16000: Function details: ${result.toString}")

	val request2 = new com.amazonaws.services.lambda.model.GetFunctionConfigurationRequest
	request2.setFunctionName(lambdaFunctionName)
	val result2 = lambdaClient.getFunctionConfiguration(request2)
	lambdaContainerMemory = result2.getMemorySize
	lambdaContainerTimeoutSecs = result2.getTimeout
	// logInfo(s"LAMBDA: 16001: Function configuration: ${result2.toString}")

	val request3 = new com.amazonaws.services.lambda.model.GetAccountSettingsRequest
	val result3 = lambdaClient.getAccountSettings(request3)
	// logInfo(s"LAMBDA: 16002: Account settings: ${result3.toString}")
  }

  override def stop(): Unit = synchronized {
    stop(SparkAppHandle.State.FINISHED)
    logInfo("stop")
  }

  override def connected(appId: String) {
    logInfo("Connected to Spark cluster with app ID " + appId)
    this.appId = appId
    notifyContext()
    launcherBackend.setAppId(appId)
  }

  override def disconnected() {
    notifyContext()
    if (!stopping) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String) {
    notifyContext()
    if (!stopping) {
      launcherBackend.setState(SparkAppHandle.State.KILLED)
      logError("Application has been killed. Reason: " + reason)
      try {
        scheduler.error(reason)
      } finally {
        // Ensure the application terminates, as we can no longer run jobs.
        sc.stopInNewThread()
      }
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
    memory: Int) {
    logInfo("Granted executor ID %s on hostPort %s with %d core(s), %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def executorRemoved(
      fullId: String, message: String, exitStatus: Option[Int], workerLost: Boolean) {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(code) => ExecutorExited(code, exitCausedByApp = true, message)
      case None => SlaveLost(message, workerLost = workerLost)
    }
    logInfo("Executor %s removed: %s".format(fullId, message))
    removeExecutor(fullId.split("/")(1), reason)
 }

  // AMAN: Using Vanilla Spark definition, will amend to use Lambda definition 
  // as well or instead of this.
  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
  }

  /*override def sufficientResourcesRegistered(): Boolean = {
    val ret = totalRegisteredExecutors.get() >= numExecutorsExpected * minRegisteredRatio
    logInfo(s"sufficientResourcesRegistered: $ret ${totalRegisteredExecutors.get()}")
    ret
  }*/

  //AMAN: Using Vanilla Spark definition instead of Spark-on-Lambda definition
  override def applicationId(): String = 
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
  }

  private def launchExecutorsOnLambda(newExecutorsNeeded: Int) : Future[Boolean] = {
    Future {
      // TODO: Can we launch in parallel?
      // TODO: Can we track each thread separately and audit
      //logInfo(s"AMAN: launchExecutorsOnLambda -> newExecutorsNeeded = $newExecutorsNeeded")
      (1 to newExecutorsNeeded).foreach { x =>
        val hostname = sc.env.rpcEnv.address.host
        val port = sc.env.rpcEnv.address.port.toString
        val currentExecutorId = executorId.addAndGet(2)
        // logInfo(s"AMAN: currentExecutorId requested -> $currentExecutorId")
        val containerId = applicationId() + "_%08d".format(currentExecutorId)

	val externalShuffle = Option(sc.getConf.get("spark.shuffle.service.enabled", "false"))

        val javaPartialCommandLine = s"java -cp ${lambdaClasspath} " +
            s"-server -Xmx${lambdaContainerMemory}m " +
            "-Djava.net.preferIPv4Stack=true " +
            s"-Dspark.driver.port=${port} " +
            "-Dspark.dynamicAllocation.enabled=true " +
            "-Dspark.shuffle.service.enabled=" + externalShuffle

        val executorPartialCommandLine = "org.apache.spark.executor.CoarseGrainedExecutorBackend " +
            s"--driver-url spark://CoarseGrainedScheduler@${hostname}:${port} " +
            s"--executor-id ${currentExecutorId} " +
            "--hostname LAMBDA " +
            "--cores 1 " +
            s"--app-id ${applicationId()} " +
            s"--user-class-path file:/mnt/shuffleDir/* " + 
            s"--executor-type LAMBDA"

        val commandLine = javaPartialCommandLine + executorPartialCommandLine

        val request = new LambdaRequestPayload(
          sparkS3Bucket = lambdaBucket.get.split("/").last,
          sparkS3Key = s"lambda/spark-lambda-${s3SparkVersion}.zip",
          sparkDriverHostname = hostname,
          sparkDriverPort = port,
          sparkCommandLine = commandLine,
          javaPartialCommandLine = javaPartialCommandLine,
          executorPartialCommandLine = executorPartialCommandLine)

        case class LambdaRequesterThread(executorId: String, request: LambdaRequestPayload)
          extends Thread {
          override def run() {
            logDebug(s"LAMBDA: 9050: LambdaRequesterThread $executorId: $request")
            // Important code: Rate limit to avoid AWS errors
            limiter.acquire()
            logInfo(s"AMAN: Log from class: LambdaRequesterThread started $executorId")
            numLambdaCallsPending.addAndGet(1)

            val invokeRequest = new InvokeRequest
            try {
              invokeRequest.setFunctionName(lambdaFunctionName)
              implicit val formats = Serialization.formats(NoTypeHints)
              val payload: String = Serialization.write(request)
              invokeRequest.setPayload(payload)
              //logInfo(s"AMAN: 9050.2: request: ${payload}")
              val invokeResponse = lambdaClient.invoke(invokeRequest)
              //logInfo(s"AMAN: 9051: Returned from lambda $executorId: $invokeResponse")
              val invokeResponsePayload: String =
                new String(invokeResponse.getPayload.array, java.nio.charset.StandardCharsets.UTF_8)
              logDebug(s"LAMBDA: 9051.1: Returned from lambda $executorId: $invokeResponsePayload")
            } catch {
              case t: Throwable => logError(s"Exception in Lambda invocation: $t")
            } finally {
              // logInfo(s"LAMBDA: 9052: Returned from lambda $executorId: finally block")
              val tmp = numLambdaCallsPending.get()
              // logInfo(s"\n\nAMAN: Reducing Pending calls from $tmp to ${numLambdaCallsPending.get()}\n\n")
              pendingLambdaRequests.remove(executorId)
              val responseMetadata = lambdaClient.getCachedResponseMetadata(invokeRequest)
              logDebug(s"LAMBDA: 9053: Response metadata: ${responseMetadata}")
            }
          }
        }

	//logInfo(s"AMAN: Starting the LambdaThread")
        val lambdaRequesterThread = LambdaRequesterThread(currentExecutorId.toString, request)
        pendingLambdaRequests(currentExecutorId.toString) = lambdaRequesterThread
        lambdaRequesterThread.setDaemon(true)
        lambdaRequesterThread.setName(s"Lambda Requester Thread for $currentExecutorId")
        //logInfo(s"AMAN: 9055: starting lambda requester thread for $currentExecutorId")
        lambdaRequesterThread.start()
        logDebug(s"LAMBDA: 9056: returning from launchExecutorsOnLambda for $currentExecutorId")
      }
      true // TODO: Return true/false properly
    }
  }

  protected override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    Option(client) match {
      case Some(c) => c.requestTotalExecutors(requestedTotal)
      case None =>
        logWarning("Attempted to request executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  override final def doRequestTotalExecutors_lambda(requestedTotal: Int, currentTotalExecutors: Int): Future[Boolean] = {
    // TODO: Check again against numExecutorsExpected ??
    // We assume that all pending lambda calls are live lambdas and are fine
    // logInfo(s"AMAN: Function call in doRequestTotalExecutors_lambda -> requestedTotal = $requestedTotal")
    val newExecutors = requestedTotal - numLambdaCallsPending.get()
    val maxExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
    val newExecutorsNeeded = math.min((maxExecutors - currentTotalExecutors), newExecutors)

    //logInfo(s"AMAN: Function call in doRequestTotalExecutors_lambda -> requestedTotal = $requestedTotal, newExecutors = $newExecutors, executorsNeeded = $newExecutorsNeeded, pendingCalls = $numLambdaCallsPending.get()")

    //logInfo(s"AMAN: 9001: doRequestTotalExecutors: newExecutorsNeeded = ${newExecutorsNeeded} " + s"currentTotalExecutors = ${currentTotalExecutors}, maxExecutors = ${maxExecutors}")
    if (newExecutorsNeeded <= 0) {
      return Future { true }
    }
    return launchExecutorsOnLambda(newExecutorsNeeded)
  } 

  protected override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    Option(client) match {
      case Some(c) => c.killExecutors(executorIds)
      case None =>
        logWarning("Attempted to kill executors before driver fully initialized.")
        Future.successful(false)
    }
  }

  override def doKillExecutorsLambda(executorIds: Seq[String]): Future[Boolean] = {
    // TODO: Right now not implemented
    // logInfo(s"AMAN: 10200: doKillExecutors: $executorIds")
    val (activeExecutors, pendingExecutors) =
      executorIds.partition(executorId => !pendingLambdaRequests.contains(executorId))
    Future {
      /*pendingExecutors.foreach { x =>
        if (pendingLambdaRequests.contains(x)) {
          logDebug(s"LAMBDA: 10201: doKillExecutors: Interrupting $x")
          val thread = pendingLambdaRequests(x)
          pendingLambdaRequests.remove(x)
          thread.interrupt()
          logDebug(s"LAMBDA: 10202: ${thread.getState}")
        }
      }*/

      //logInfo(s"AMAN: List for executors getting passed to super function -> $executorIds")
      super.doKillExecutorsLambda(executorIds)
      true
    }
  }

  private val lambdaSchedulerListener = new LambdaSchedulerListener(scheduler.sc.listenerBus)
  private class LambdaSchedulerListener(listenerBus: LiveListenerBus)
    extends SparkListener with Logging {

    listenerBus.addListener(this)

    override def onExecutorAdded(event: SparkListenerExecutorAdded) {
      logDebug(s"LAMBDA: 10100: onExecutorAdded: $event")
      logDebug(s"LAMBDA: 10100.1: onExecutorAdded: ${event.executorInfo.executorHost}")
      logDebug(s"LAMBDA: 10100.2: ${numExecutorsRegistered.get}")
      logDebug(s"LAMBDA: 10100.4: ${numLambdaCallsPending.get}")
      // TODO: synchronized block needed ??
      liveExecutors.add(event.executorId)
      numExecutorsRegistered.addAndGet(1)
    }
    override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
      logDebug(s"LAMBDA: 10101: onExecutorRemoved: $event")
      logDebug(s"LAMBDA: 10101.2: $numExecutorsRegistered")
      logDebug(s"LAMBDA: 10101.4: ${numLambdaCallsPending.get}")
      liveExecutors.remove(event.executorId)
      numExecutorsRegistered.addAndGet(-1)
    }
  }

   // AMAN: Commenting this function because most if not all of this is common with
   // the super definition.
   /**
    * Override the DriverEndpoint to add extra logic for the case when an executor is disconnected.
    * This endpoint communicates with the executors and queries the AM for an executor's exit
    * status when the executor is disconnected.
  private class LambdaDriverEndpoint(rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends DriverEndpoint(rpcEnv, sparkProperties) {

    // TODO Fix comment below
      * When onDisconnected is received at the driver endpoint, the superclass DriverEndpoint
      * handles it by assuming the Executor was lost for a bad reason and removes the executor
      * immediately.
      *
      * In YARN's case however it is crucial to talk to the application master and ask why the
      * executor had exited. If the executor exited for some reason unrelated to the running tasks
      * (e.g., preemption), according to the application master, then we pass that information down
      * to the TaskSetManager to inform the TaskSetManager that tasks on that lost executor should
      * not count towards a job failure.
    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      logDebug(s"LAMBDA: 10001: onDisconnected: $rpcAddress")
      super.onDisconnected(rpcAddress)
      logDebug("LAMBDA: 10002: onDisconnected")
    }
  }
 
  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    logDebug("LAMBDA: 10001: createDriverEndPoint: LambdaDriverEndpoint")
    new LambdaDriverEndpoint(rpcEnv, properties)
  }
      */
 
  // AMAN: We probably don't need this function, we have to discuss the case
  // where if we don't have any initial executors on VMs, do we launch executors
  // on Lambdas, or wait for the cluster to have at least one VM

  // Currently, we don't wait for any VM executor. If currently no workers are 
  // present, we can directly launch Lambda executors if other settings allow 
  // for it.
  private val DEFAULT_NUMBER_EXECUTORS_LAMBDA = 1
  private def getInitialTargetExecutorNumber(
                                      conf: SparkConf,
                                      numExecutors: Int = DEFAULT_NUMBER_EXECUTORS_LAMBDA): Int = {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
      val initialNumExecutors =
        Utils.getDynamicAllocationInitialExecutors(conf)
      val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
      require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      val targetNumExecutors =
        sys.env.get("SPARK_EXECUTOR_INSTANCES").map(_.toInt).getOrElse(numExecutors)
      // System property can override environment variable.
      conf.get(EXECUTOR_INSTANCES).getOrElse(targetNumExecutors)
    }
   }

  private def waitForRegistration() = {
    registrationBarrier.acquire()
  }

  private def notifyContext() = {
    registrationBarrier.release()
  }

  private def stop(finalState: SparkAppHandle.State): Unit = synchronized {
    try {
      stopping = true

      super.stop()
      client.stop()

      val callback = shutdownCallback
      if (callback != null) {
        callback(this)
      }
    } finally {
      launcherBackend.setState(finalState)
      launcherBackend.close()
    }
  }

  // AMAN: We don't need this function because we are not working
  // with two different endpoints.
 
  /*
  *
  private[spark] object LambdaSchedulerBackend {
    val ENDPOINT_NAME = "LambdaScheduler"
  }
  */
}
