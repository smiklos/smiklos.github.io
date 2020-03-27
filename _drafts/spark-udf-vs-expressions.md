---
layout: post
title: Spark UDF vs sql expressions
description: Performance characteristics and nullability when working with UDFs
---


1. Introduction to spark udf and expressions
2. spark internal storage format ints, utf8, unsafe
3. Generated code analysis of udf vs expressions on int + 1
4. Generated code analysis of udf vs expressions on string + lit(" hi")
5. UDf vs expressions conclusion?
6. Udf nullability and predicate pushdown. Udf null input protection
7. Possible improvements -> Specialization implemented for codegen. Check also the input types, not just the udf function inputs
8. Conclusion and recommendations for spark udf and expressions. Don't use it unless you want to convert from internal row straight to something


There are plenty of articles explaining how to use udfs in spark and what benefits/drawbacks they have over built-in/native functions.
In this post we will look at the internals and compare udfs against built in native functions from an implementation point of view.
Wi will look at the following topics:

* How udfs are treated by spark when it comes to code generation
* How to achieve isNull predicate pushdown to parquet or other file sources
* When should we implement native functions


# A short 101 on spark sql internals

When spark sql executes a query, it takes all the necessary computation that we have declared in our program
and after some optimization, it will end up generating java code out of it. This is done to better utilize the CPU.
Additionally, our data is not stored in memory as regular java/scala objects are but rather kept in an efficient compacted way
to reduce memory usage and improve CPU caching. This means that an ideal program will try to keep this data in spark's format for as long as possible,
otherwise intermediate data representations are needed to convert back and forth between java/scala types and spark's internal format.
We will look at this in more detail when looking at the overhead associated with udfs.


# Comparing UDF codegen behavior against native functions

The following examples were based on spark version 2.4.4.


```scala
 val df = spark.sparkContext
    .parallelize(Seq(42))
    .toDF("num")
    .withColumn("num_plus_one", 'num.plus(1))

  df.queryExecution.debug.codegen()
````

The above snippet will generate roughly 60 lines of code, but will focus on the actual transformation for brevity.

```java
/* 026 */   private void project_doConsume_0(int project_expr_0_0) throws java.io.IOException {
/* 027 */     int project_value_2 = -1;
/* 028 */     project_value_2 = project_expr_0_0 + 1;
/* 029 */     serializefromobject_mutableStateArray_0[2].reset();
/* 030 */
/* 031 */     serializefromobject_mutableStateArray_0[2].write(0, project_expr_0_0);
/* 032 */
/* 033 */     serializefromobject_mutableStateArray_0[2].write(1, project_value_2);
/* 034 */     append((serializefromobject_mutableStateArray_0[2].getRow()));
/* 035 */
/* 036 */   }
```

From the above snippet we can derive a couple of things:
* `project_expr_0_0` is the value of the field 'num'
* There are no null checks since we work with primitive values. The result of our function can't be null therefor
* After the computation is done, both `num` and `num_plus_one` gets written to the output row

Now lets see what we get when we implement the same functionality with a udf.

```scala
 val df = spark.sparkContext
    .parallelize(Seq(42))
    .toDF("num")
    .withColumn("num_plus_one", udf((in: Int) => in + 1).apply('num))

  df.queryExecution.debug.codegen()
````

And the corresponding generated code:

```java
/* 026 */   private void project_doConsume_0(int project_expr_0_0) throws java.io.IOException {
/* 027 */     Object project_arg_0 = false ? null : ((scala.Function1[]) references[0] /* converters */)[0].apply(project_expr_0_0);
/* 028 */
/* 029 */     Integer project_result_0 = null;
/* 030 */     try {
/* 031 */       project_result_0 = (Integer)((scala.Function1[]) references[0] /* converters */)[1].apply(((scala.Function1) references[2] /* udf */).apply(project_arg_0));
/* 032 */     } catch (Exception e) {
/* 033 */       throw new org.apache.spark.SparkException(((java.lang.String) references[1] /* errMsg */), e);
/* 034 */     }
/* 035 */
/* 036 */     boolean project_isNull_2 = project_result_0 == null;
/* 037 */     int project_value_2 = -1;
/* 038 */     if (!project_isNull_2) {
/* 039 */       project_value_2 = project_result_0;
/* 040 */     }
/* 041 */     serializefromobject_mutableStateArray_0[2].reset();
/* 042 */
/* 043 */     serializefromobject_mutableStateArray_0[2].zeroOutNullBytes();
/* 044 */
/* 045 */     serializefromobject_mutableStateArray_0[2].write(0, project_expr_0_0);
/* 046 */
/* 047 */     serializefromobject_mutableStateArray_0[2].write(1, project_value_2);
/* 048 */     append((serializefromobject_mutableStateArray_0[2].getRow()));
/* 049 */
/* 050 */   }
```

As we can see, there's quite a lot more going on here. Lets talk about what this piece of code does.

* First there's a conversion applied to the input value. In the case of a single Int, this is redundant as our udf takes the same type as input.
* Next, our udf gets called with the converted value as input. We can see that an explicit casting to boxed type takes place.
* We can see that in case our udf returns a null value, we get the default value of -1 assigned to the result.
* From writing the values out, the only difference is the call to `.zeroOutNullBytes()`. This method indicates that it's possible to put null values to the buffer and so it's an extra setup to be done after resetting the buffer.

From the above we can clearly see that there's an overhead of input value conversion, primitive type to boxed type casting and additional null checks and nullability related cleanups.

