## My blog

Upcoming posts

* The real cost of spark UDF
  * A look at what code gets genearted for udf vs expressions
  * What we should look for with regards to nullability
  * Should we roll our own expression? Considerations vs udf

* Operator fusion in different technologies
  * Spark
  * Flink
  * Kafka Streams
  * Reactor
  * Akka Streams

* Spark code gen deep dive
  * Physical stages
  * Expressions

* Spark sql execution with example for each stage
  * Parsed plan
  * Logical plan
  * Optimized plan
  * Physical plan
  * Executed plan

* Error handling patterns
  * Regular try catch
  * Error code, double return value in go
  * Functional types in scala like Try and IO monads
  * Supervisor strategy in Akka/Erlang
  * Streaming error handling with reactor/akka streams

* Cumulative subaggregation elimination
  * Problem statement
  * Naive solution with double shuffle
  * Better solution with nesting (what if it's too much data or we can't control the structure?)
  * Handmade solution with foreach partition
  * Custom subexpression like min of avg, max of avg
  * Query optimizer to automate this and eliminate nested aggregation (collapse)

* Map side join techniques in structured streaming
  * Brodcasted join in spark streaming (static to stream)
  * Kafka global and local table example to show
  * udf mapper with reused brodcast and update on batch etc.
  * Hollow based udf mapper for spark. In general, a solution for small tables without change support
  * Kafka streaming update with based approach

* Functional evolution
  * regular functions
  * Monads as upgrade
  * Monad transformers

* ZIO
  * More on imperative vs high level code, parallelism, exception handling, fail fast, pass through
  * Digaram of app dependencies and interraction with side effecting stuff. like a hierarchy
  * Fork, repeat etc.
  * PArallel to reactive streams (threading, api. )
  * Compare to reactive libs like reactor/rxjava
