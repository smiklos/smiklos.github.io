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
3. using the sparkContext's or the SparkSession builder's config method like `.config("spark.important.config.value", "false")
`
Now the namespace is basically the prefix to every metric sent to statsd, so it can be the same as our application name provided that we use a concise name without spaces.
This step is optional, but I highly recommend it

```
.config("spark.metrics.namespace", "my-app")

```

Then we need to configure the sinks. The config for them are loaded from a property file that can be found by default under 
```
$SPARK_HOME/conf/metrics.properties
```

Alternatively we can point to another file with 

```
.config("spark.metrics.conf", "/home/centos/spark/config/metrics.properties")

```

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

This will enable all built in metrics except from metrics from structured streaming. 

## Enable metrics for structured streaming queries

