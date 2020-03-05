---
layout: post
title: Exposing Structured Streaming metrics from Spark
---

### Spark version: 2.4.4

We will cover the following topics

* Enable and configure metric reporters for statsd
* Enable metrics for structured streaming queries
* Hack together a custom metrics source

# The problem 

While spark exposes some metrics via api-s and other sinks, not all of them are turned on by default and there's no built in support to include custom metrics.
Spark 3.0 changes this thanks to [this pr](https://github.com/apache/spark/pull/24901)

We've been using some hand written library and a combination of accumulators to send metrics directly to our ELK stack and it was time to look into something better.
These metrics were very limited in granularity and we could only send them from the drivers once the job either finished or a processing window completed.

Let's see what needs to be configured in order to enable metric reporting for built in metrics to a statsd server

## Enable and configure metric reporters for statsd

First and foremost, we need to set up a namespace for the metrics, otherwise spark defaults to the random app id and that's rarely what we want.

There a several ways to configure a spark application, just a few possiblities: 

1. via the default config file under `$SPARK_HOME/conf/spark-defaults.conf`
2. as a config parameter passed to spark-submit, e.g.: `--conf spark.important.config.value=false`
3. using the sparkContext's or the SparkSession builder's config method like `.config("spark.important.config.value", "false")`

Now the namespace is basically the prefix to every metric sent to statsd, so it can be the same as our application name provided that we use a concise name without spaces.
This step is optional, but I highly recommend it

`.config("spark.metrics.namespace", "my-app")`

Then we need to configure the sinks. The config for them are loaded from a property file that can be found by default under 
```
$SPARK_HOME/conf/metrics.properties
```

Alternatively we can point to another file with 

`.config("spark.metrics.conf", "/home/centos/spark/config/metrics.properties")`

To enable statsd, this is what the file should contain

```
# org.apache.spark.metrics.sink.StatsdSink
#   Name:     Default:      Description:
#   host      127.0.0.1     Hostname or IP of StatsD server
#   port      8125          Port of StatsD server
#   period    10            Poll period
#   unit      seconds       Units of poll period
#   prefix    EMPTY STRING  Prefix to prepend to metric name

*.sink.statsd.class=org.apache.spark.metrics.sink.StatsdSink

```

This will enable all built in metrics except from metrics coming from structured streaming queries, so lets look at that next. 

## Enable metrics for structured streaming queries

Lets start with yet another optional but in general recommended step, adding a queryName to the stream so it's not a random UUID that we get but rather a constant name we can easily track across restarts.

```
df.trigger(Trigger.ProcessingTime("5 seconds"))
        .option("queryName", "important-query")
        .start()
```

After that, we just need to enable structured streaming metrics in via the following config:

`.config("spark.sql.streaming.metricsEnabled", "true")`

Now we can get streaming query metrics to statsd (or any other sink we configure)

As I already mentioned, it's not possible to extend spark's metric system before version 3.0 so we need to use a bit of cheat to hook into it.

## Hack together a custom metrics source

### While the following definitely works, take it as an optional approach 

We need to implement and register an instance of the `Source` trait in the package `org.apache.spark.metrics.source`
Since this trait is package private, we have to put our implementation under the same package.

```
package org.apache.spark.metrics.source
import com.codahale.metrics.MetricRegistry
import org.apache.spark.sql.SparkSession

class CustomAppMetrics extends Source {
  override def sourceName: String = "custom"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val customMetric = metricRegistry.timer(MetricRegistry.name("custom-time"))
}

object CustomAppMetrics {

  def register(sparkSession: SparkSession) = {
    val source = new CustomAppMetrics
    sparkSession.sparkContext.env.metricsSystem
      .registerSource(source)
    source
  }
}

```

I included a helper method to register and return the metric source instance in one go. 

Usage is simple when we are working with the driver but since spark doesn't have any initialization phase we would need to register this instance on each worker by some spark job like `sc.parallelize(0 to 100).forEach(register)`

Additionaly, we need to have jetty-servlets on the compile class path for this to work... 
`"org.eclipse.jetty" % "jetty-servlets" % "9.4.6.v20180619" % "provided"`

Now we are ready to create more metrics and use them on the driver or workers or both and gain access to the built in reporting functionality of spark. 

Links

[Monitoring guide](https://spark.apache.org/docs/latest/monitoring.html)
