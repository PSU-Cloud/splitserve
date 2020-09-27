# SplitServe

SplitServe is a framework to efficiently run stateful workloads on
both IaaS and FaaS. 
SplitServe is built upon Apache Spark.

To use SplitServe with AWS Lambdas, you will need to setup Lambdas
to work with your AWS cluster.
[Instructions to setup AWS Lambdas](https://docs.google.com/document/d/1ZheYDby7ZeJ69THJVyR3gNsFNLVbb3whUfaG8mYS-iE/edit?usp=sharing)

Lambda function code can be found under `bin/LambdaCode/lamda_function.py`

You will also need to setup HDFS to allow SplitServe executors
to read and their their intermediate shuffle data to and from a
common layer. We recommend using HDFS-2.7.7

## Building SplitServe
To build SplitServe, use the following:
```
./build/mvn -DskipTests clean package
```

After building SplitServe, zip the libraries to run SplitServe
executor binary and upload them to your S3 bucket. When SplitServe
is run for the first time while using Lambda executors, it will
download the executors (and any other required libraries) from this
bucekt.

## Adding configurations
SplitServe supports almost all basic Spark configurations.
A list of these configurations with descriptions can be
found [here](https://spark.apache.org/docs/2.1.0/configuration.html)

These configruations must be added to `spark-defaults.conf` in the conf
directory or given as command line arguments.

SplitServe supports various other configurable knobs to control
the cluster configuration to suit the workload being run.

In order to use AWS Lambdas as executors the following configurations
are needed to let SplitServe know how to use Lambdas:

This configuration is needed to use Lambdas alongside VMs as executors.
If the the scheduler finds pending tasks but no available executors
in the cluster, SplitServe will autoscale using either VMs or Lambdas.
On the other hand, since Lambdas have various resource limitations on
them, SplitServe will release Lambdas not being used and/or if they are
nearing their resource caps.

```
spark.dynamicAllocation.enabled=true
```

Number of executors a cluster can work with to run a workload.
Note, this number represents the sum of all executors in the cluster
(i.e. VM and Lambda executors).

```
spark.dynamicAllocation.minExecutors=1
spark.dynamicAllocation.maxExecutors=8
```

To let SplitServe know how long to use a Lambda executor for before
gracefully releasing them:

```
spark.lambda.executor.timeout=120s
spark.lambda.concurrent.requests.max=500
```


When working with Lambda executors, SplitServe needs a place to
let the executors write their intermediate shuffle data. SplitServe
uses HDFS to facilitate this. A common HDFS layer is shared between
both VM and Lambda executors to read and write the shuffle data.
Using HDFS instead of any other external storage solution makes
SplitServe more cost efficient as well as performant.

```
spark.shuffle.hdfs.enabled=true
spark.shuffle.hdfs.node=hdfs://<hdfs_primary_node_ip>:<hdfs_primary_node_port>
```

Name of the Lambda function in AWS Lambda portal:

```
spark.lambda.function.name=spark-lambda
```

AWS S3 credentials to download the SplitServe executor code and other
libraries you might want to use with SplitServe Lambda executors:

```
spark.hadoop.fs.s3n.awsAccessKeyId=<aws_access_key_id>
spark.hadoop.fs.s3n.awsSecretAccessKey=<aws_secret_access_key>
```

These configurations are optional but are helpful to log details for
invidual components of the cluster for detailed analysis:

```
spark.eventLog.enabled true
spark.eventLog.dir <path_to_log_directory>
spark.history.fs.logDirectory <path_to_history_directory>
```

## Example:
After building SplitServe, setup your AWS Lambda function to be used as executors by SplitServe.
(Note that this is optional. You can run SplitServe with only VMs in the cluster to be used
as executors as well).

1. Start HDFS on the cluster and give the primary node's address and port
as a configuration either through `spark-defaults.conf` or as a
command line argument.

2. Load the binaries in your S3 bucket (if using Lambdas).

3. Start SplitServer master:
```
SPLITSERVE-HOME/sbin/start-master.sh
```

4. Run the following example to calculate value of Pi with 4
cores:
```
SPLITSERVE-HOME/bin/spark-submit --class org.apache.spark.examples.SparkPi --master <SPLITSERVE-MASTER-IP> SPLITSERVE-HOME/examples/target/original-spark-examples_2.11-2.1.0.jar 4
```
