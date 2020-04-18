---
layout: post
title: The real cost of Spark UDF
description: Performance characteristics and nullability when working with UDFs
---

There are plenty of articles explaining how to use UDFs in Spark and what benefits/drawbacks they have over native functions.
In this post we will look under the hood of Spark UDFs to get an understanding of the performance overhead they incur.

> The following article is based on Spark version 2.4.4.

### A short explanation on Spark's internal data representation


When Spark sql executes a query, it takes all the necessary computation that we have declared in our program
and after some optimization it will end up generating java code out of it. This is done to better utilize the CPU by removing unnecessary method calls.
Additionally, our data is not stored in memory as regular java/scala objects are but rather kept in an efficient compacted way
to reduce memory usage and improve CPU caching. To avoid having to convert data when a computation happens inside Spark sql,
Spark's built in functions often operate directly on this internal format.
UDFs on the other hand were meant to be created by users of Spark who are familiar with the built-in types the JVM provides.
In order for our UDFs to work, Spark needs to do conversions from the internal format to the standard JVM types so that our functions can be invoked by data stored inside Spark's memory.

Lets take a look at how this conversion is handled by Spark in more detail.

### Comparing UDF codegen behavior against native functions

One of the easiest way to see what's under the hood is to look at the generated code Spark will execute based on our transformations.
It's often enough to check the physical plan to get an understanding of how our job will be executed by Spark but to see how the data is computed it's better to look at the generated code as
it allows us to see the whole picture better when there are multiple transformations going on in the same physical plan.

We will be working with a single parquet file that contains a single string column called _str_
and can be generated via the following snippet:

```scala
  spark.sparkContext
    .parallelize(0 to 10000000)
    .map(i => s"text-$i")
    .toDF("str")
    .coalesce(1)
    .write
    .parquet("path/to/data/")
```

Now that we have some data, lets see what code Spark generates when we use a built in function called `substring`.

```scala
val stringParquetDF = 
  spark.read.parquet("path/to/data/")
  .withColumn("str", substring('str, 0, 2))

stringParquetDF.queryExecution.debug.codegen()
````

And here is the stripped down version of the output, only showing the relevant code segment.

```java
/* 026 */       InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();
/* 027 */       boolean project_isNull_0 = true;
/* 028 */       UTF8String project_value_0 = null;
/* 029 */       boolean inputadapter_isNull_0 = inputadapter_row_0.isNullAt(0);
/* 030 */       UTF8String inputadapter_value_0 = inputadapter_isNull_0 ?
/* 031 */       null : (inputadapter_row_0.getUTF8String(0));
/* 032 */       if (!inputadapter_isNull_0) {
/* 033 */         project_isNull_0 = false; // resultCode could change nullability.
/* 034 */         project_value_0 = inputadapter_value_0.substringSQL(0, 2);
/* 035 */
/* 036 */       }
```

Lets walk through the above snippet to better understand what the types mean and what's happening.

The first 4 lines corresponds to acquiring the next row from the task's row iterator.  
Here the type `InternalRow` is essentially a pointer to the internal data structure used by Spark to hold the values of each column inside the row.
Since by default when we read data from parquet, all columns are nullable and therefor a null check is done to make sure we don't call the substring function on null values.
On line 30 we get the value of the column _str_ from our row. The type `UTF8String` is how Spark represents Strings inside its in-memory data structure.
After a `null` check against the input `UTF8String` column value the method `substringSQL` is called on `UTF8String`.

From the above we can see that Spark manipulates its internal data structure directly and that there's no conversion needed to regular java/scala `String` object.
[todo] It's also worth to note that there's no error handling done on the `substringSQL` function, indicating that it can't fail on non `null` input.
This technique is used across many of Spark's built-in functions.

Now lets see what we get when we implement the same functionality using a UDF.

```scala
val stringParquetDF = 
  spark.read.parquet("path/to/data/")
  .withColumn("str", udf((in: String) => in.substring(0, 2)).apply('str))

stringParquetDF.queryExecution.debug.codegen()
````

And the corresponding generated code:

```java
/* 026 */       InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();
/* 027 */       boolean inputadapter_isNull_0 = inputadapter_row_0.isNullAt(0);
/* 028 */       UTF8String inputadapter_value_0 = inputadapter_isNull_0 ?
/* 029 */       null : (inputadapter_row_0.getUTF8String(0));
/* 030 */       Object project_arg_0 = inputadapter_isNull_0 ? null : ((scala.Function1[]) references[0] /* converters */)[0].apply(inputadapter_value_0);
/* 031 */
/* 032 */       UTF8String project_result_0 = null;
/* 033 */       try {
/* 034 */         project_result_0 = (UTF8String)((scala.Function1[]) references[0] /* converters */)[1].apply(((scala.Function1) references[2] /* udf */).apply(project_arg_0));
/* 035 */       } catch (Exception e) {
/* 036 */         throw new org.apache.spark.SparkException(((java.lang.String) references[1] /* errMsg */), e);
/* 037 */       }
/* 038 */
/* 039 */       boolean project_isNull_0 = project_result_0 == null;
/* 040 */       UTF8String project_value_0 = null;
/* 041 */       if (!project_isNull_0) {
/* 042 */         project_value_0 = project_result_0;
/* 043 */       }
```

As we can see, there's quite a lot more going on here so lets walk through this piece of code as well.
After getting the value of the _str_ column on line 28 there's a conversion applied to the input value on line 30.
In the case of `UTF8String` this conversion means the that the column needs to be converted into a `String` object,
as that's the type of the input variable our UDF expects.
On line 34, our UDF gets called and another conversion takes place to convert the return value back to a `UTF8String`.
This is because our UDF returns a `String` value that Spark is aware of and therefor knows what conversion it needs to apply to turn the value back into a `UTF8String`.
The UDF invocation is also wrapped in a _try/catch_  block and in case of an exception, a user friendly error message is shown along with the original exception.

From the above we can clearly see that there's substantial overhead of type conversion involved in the process as well as additional nullability checks.
How substantial? Well, apart from the memory overhead that the conversion creates we can easily test the raw performance of the computation.

|    Rows    |   Columns   | Expr substring | UDF substring |
|------------|-------------|:--------------:|--------------:|
| 10,000,000 |      1      |    38.97 ms    |    51.07 ms   |
| 10,000,000 |      5      |    41.43 ms    |    116.83 ms  |


From the above we can derive that the execution overhead of spark was around 38ms and everything above that accounts for the computation done on the _str_ column.

## UDFs and nullability

Now that we looked at the general performance characteristics, it's time to look at how UDF inputs and outputs are being handled by Spark.
In general UDFs can both receive and return primitive values as well as objects. Depending on whether we communicate our function's null handling behavior can have a big impact on the performance of our application.
Lets look at this in a bit more detail.

### Predicate pushdown with UDFs

Predicate pushdown is a technique used in data computing systems like Spark to filter out data before it's even read from the disk.  
File formats such as Parquet and ORC support this functionality and it can tremendously speed up our processing by eliminating unnecessary IO operations.
In its simplest form filters like equality checks, non null checks and comparison against some static value can be pushed down and the supported data source will skip reading data from files where the predicate doesn't match.  
As of now Spark doesn't handle predicate pushdown when using UDFs. While equality checks are hard to support since we have no insight into what's happening inside the function,
`null` checks should be possible to be pushed down. The easiest way to do this is to add additional filtering on `null` columns that are sent as input value to the `UDF`.
Lets see what the physical plan looks like when trying to match th return value against a literal using built in functions, UDFs and UDFs with additional nonNull filter.

Using built in functions to filter data we get `IsNotNull` predicate pushdown.

```scala
stringParquetDF.filter(substring('str, 0, 2).equalTo("test")).explain()
```

```
== Physical Plan ==
*(1) Project [str#11]
+- *(1) Filter (isnotnull(str#11) && (substring(str#11, 0, 2) = test))
   +- *(1) FileScan parquet [str#11] Batched: true, Format: Parquet, 
   Location: InMemoryFileIndex[file:/home/../test-data.parquet], PartitionFilters: [], 
   PushedFilters: [IsNotNull(str)], ReadSchema: struct<str:string>
```

Unfortunately using our UDF won't yield any predicate pushdown.

```scala
  stringParquetDF.filter(udf((in: String) => in.substring(0, 2)).apply('str).equalTo("test")).explain(true)
```

```
== Physical Plan ==
*(1) Filter (UDF(str#11) = test)
+- *(1) FileScan parquet [str#11] Batched: true, Format: Parquet,
Location: InMemoryFileIndex[file:/home/../test-data.parquet], PartitionFilters: [],
PushedFilters: [], ReadSchema: struct<str:string>
```

Since our function expects an input of type `String` it can receive `null` values and still produce non `null` results.
That's enough for spark not to filter out records that contain null for the column _str_.
In case we need to handle non primitive input types such as `String` or `Array[Byte]` but we don't want to handle `null` input
values, we should explicitly filter the input column for non nullability which will bring back the missing predicate pushdown we greatly appreciate.


```scala
  stringParquetDF.filter('str.isNotNull and udf((in: String) => in.substring(0, 2)).apply('str).equalTo("test")).explain(true)
```

```
== Physical Plan ==
*(1) Project [str#11]
+- *(1) Filter (isnotnull(str#11) && (UDF(str#11) = test))
   +- *(1) FileScan parquet [str#11] Batched: true, Format: Parquet, 
   Location: InMemoryFileIndex[file:/home/smiklos/work/blog-playground/data/udf/test-data.parquet], PartitionFilters: [], 
   PushedFilters: [IsNotNull(str)], ReadSchema: struct<str:string>
```

Furthermore, Spark automatically protects our UDFs from receiving `null` values in case the input of the function expects a primitive type by inserting an `IsNull` expression.
Sadly, even though we protect against `null` values in such cases we still don't get the desired predicate pushdown in conjunction of a filter expression
when we clearly do not need any rows from the datasource where the column used as input value of the UDF is null.
Currently the optimizer doesn't handle this case but we could roughly implement an optimizer rule for this case by the following code.

```scala
object FilterUDFInputsNonNull extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Filter(fc, child) =>
      Filter(splitConjunctivePredicates(fc).map {
        case e @ EqualTo(If(IsNull(_), _, s: ScalaUDF), l @ Literal(litVal, dataType)) =>
          // if none of the inputs handle null, all the inputs are of primitive types
          val safeToPushdown = s.inputsNullSafe.forall(i => i)
          val udfCallWithoutIsNull = EqualTo(s, l)
          if (safeToPushdown) {
            s.children.map(IsNotNull).reduceOption(And)
              .map(filters => And(filters, udfCallWithoutIsNull))
              .getOrElse(udfCallWithoutIsNull)
          } else e
        case other => other
      }.reduce(And), child)
  }
}
```

While this is certainly not a production ready implementation, the idea is that if all inputs are protected from receiving `null`
and the value against which we compare the result of the UDF is not `null`, it's safe to replace `IsNull` with an `isNotNull` filter.

### UDF output nullability

Another thing to keep in mind when working with UDFs is that if we end up returning a non primitive value like `String`,
the expression will be nullable which will affect the performance of any further computation that depends on the output of this UDF.
It turns out that we can tell Spark to mark our UDF non-nullable and we should do so if we know that the function will never return `null`.  
Instead of looking at the extra lines of code generated to protect against null values, we can just look at what happens to the schema when depending on a column that is nullable.

```scala
stringParquetDF
    .withColumn("sub_str", udf((in: String) => in.substring(0, 2)).apply('str))
    .withColumn("first_char", substring($"sub_str", 0 , 1))
    .withColumn("last_char", substring($"sub_str", -1 , 1))
    .printSchema()
```

```
root
 |-- str: string (nullable = true)
 |-- sub_str: string (nullable = true)
 |-- first_char: string (nullable = true)
 |-- last_char: string (nullable = true) 
```

As we can see, depending on a nullable column will make all dependent column nullable as well.
Nullability is important for Spark to quickly check if a value is `null` or not therefor has to be checked every time a value is written back to a `Row`.
If we know that our UDF can't produce a `null` value, we can prevent `null` propagation by marking the UDF as non-nullable.

```scala
stringParquetDF
    .withColumn("sub_str", udf((in: String) => in.substring(0, 2)).asNonNullable().apply('str))
    .withColumn("first_char", substring($"sub_str", 0 , 1))
    .withColumn("last_char", substring($"sub_str", -1 , 1))
    .printSchema()
```

```
root
 |-- str: string (nullable = true)
 |-- sub_str: string (nullable = false)
 |-- first_char: string (nullable = false)
 |-- last_char: string (nullable = false) 
```

So, how does this affect performance?

|    Rows    |  Nullable UDF  | Non Nullable UDF |
|------------|:--------------:|-----------------:|
| 10,000,000 |    37.85 ms    |    35.35 ms      |

It's not all too bad but if you really want to squeeze out what you can from your cluster, this is something to keep in mind.

### Final thoughts

As we have seen that there's a great deal of overhead when using UDFs in Spark. UDFs are not only hard to optimize for
but they force Spark to apply additional conversions so the user experience doesn't suffer by working with unfamiliar, internal data types.
There's still a few things to improve when it comes to query optimization and code generation, just to name a few:

* Optimize predicate pushdown in relation to filtering against non null values
* Apply specialization if the return type is primitive to avoid boxing
* Remove conversions when working with primitive values

In general, **always seek to use built-in functions over UDFs** for simple calculations as
Spark will be able to do optimizations and over time with new releases our code might get faster on its own.
Only when the resulting expression would be too inefficient to execute does it make sense to use a UDF or if we need to access data broadcasted from the driver.
When working with complex or binary data it might make sense to develop our own native function to avoid unnecessary conversions.  
A good example of this is the avro [conversion](https://github.com/apache/spark/blob/branch-2.4/external/avro/src/main/scala/org/apache/spark/sql/avro/CatalystDataToAvro.scala#L49) done in Spark 2.4 where the conversion to avro blobs happen directly from Spark's internal format

