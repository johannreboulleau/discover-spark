# Spark learning

My learning of Spark is based
on the
book "[Learning Spark: lighting-fast data analytics](https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/)".

The authors are co-founder of Databricks and a committer on Apache Spark.

## Installation

Download spark from https://spark.apache.org/downloads.html

And decompress it in this folder.

## Spark components

![spark-components.png](images/spark-components.png)

## Ecosystem

![spark-ecosystem.png](images/spark-ecosystem.png)

## Spark architecture

![spark-architecture.png](images/spark-architecture.png)

* Spark driver is responsible for instantiating a SparkSession, it has multiple
  roles: it communicates with the cluster manager; it requests resources (CPU, memory, etc.) from the cluster manager
  for Spark’s executors (JVMs); and it transforms all the Spark operations into DAG computations, schedules them, and
  distributes their execution as tasks across the Spark executors.
* SparkSession became a unified conduit to all Spark operations and data.
* Cluster manager is responsible for managing and allocating resources for the cluster of nodes on which your Spark
  application runs.
* Spark executors are responsible for executing tasks on the workers.

## Deployment modes

* Local
* Standalone
* YARN
* Kubernetes

## Get started

### Launching Spark

* `./bin/spark-shell`
* ./bin/pyspark

And to read a file

```sh
scala> val strings = spark.read.text("../README.md")
strings: org.apache.spark.sql.DataFrame = [value: string]

scala> strings.show(10, false)
+------------------------------------------------------------------------------+
|value                                                                         |
+------------------------------------------------------------------------------+
|# Apache Spark                                                                |
|                                                                              |
|Spark is a unified analytics engine for large-scale data processing. It       |
|provides high-level APIs in Scala, Java, Python, and R, and an optimized      |
|engine that supports general computation graphs for data analysis. It also    |
|supports a rich set of higher-level tools including Spark SQL for SQL and     |
|DataFrames, MLlib for machine learning, GraphX for graph processing,          |
| and Structured Streaming for stream processing.                              |
|                                                                              |
|<https://spark.apache.org/>                                                   |
+------------------------------------------------------------------------------+
only showing top 10 rows

scala> strings.count()
res2: Long = 109
```

### Concepts

![spark-concepts.png](images/spark-concepts.png)

* Application: A user program built on Spark using its APIs. It consists of a driver program and executors on the
  cluster.
* SparkSession: An object that provides a point of entry to interact with underlying Spark functionality and allows
  programming Spark with its APIs. In an interactive Spark shell, the Spark driver instantiates a SparkSession for you,
  while in a Spark application, you create a SparkSession object yourself.
* Job: A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action (e.g.,
  save(), collect()).
* Stage: Each job gets divided into smaller sets of tasks called stages that depend on each other.
* Task: A single unit of work or execution that will be sent to a Spark executor.

NB: RDD (Resilient Distributed Dataset) is a Spark abstraction that represents an immutable distributed collection of
objects that can be processed in parallel.

### Lazy transformations and eager actions

Transformations are lazy, and wait an action to be executed.

Transformations are filter, map, reduce, etc.

Actions are collect, count, etc.

Example in Python

```sh
# In Python 
>>> strings = spark.read.text("../README.md") # => lazy
>>> filtered = strings.filter(strings.value.contains("Spark")) # => lazy
>>> filtered.count() # => eager
20
```

### Narrow and Wide Transformations

![narrow-wide-dependencies.png](images/narrow-wide-dependencies.png)

Transformations can be classified into:

* narrow transformations: `filter()`, `contains()`
* wide transformations. `groupBy()`, `orderBy()`

### Spark ui

To inspect Spark applications: http://localhost:4040/

THe UI is launched by the driver.

We can also add eventLog parameters to record the events and read the logs in Spark ui
with `./sbin/start-history-server.sh`.

## Spark Standalone applications

Code is in `chapter2-mnmcount/`:

* script `mnmcount.py`

To launch the application:

```sh
cd spark-4.1.1-bin-hadoop3/
./../spark-4.1.1-bin-hadoop3/bin/spark-submit \
  --conf spark.eventLog.enabled=true \ # important to record the events and read the logs
  --conf spark.eventLog.dir=file:/tmp/spark-events \
  mnmcount.py mnm_dataset.csv
```

Read the log in Spark ui by executing

```shell
cd /home/reboulleau/repos/discover-spark/spark-4.1.1-bin-hadoop3
./sbin/start-history-server.sh
```

We can see the DAG, execution time, tasks, stages, etc.
