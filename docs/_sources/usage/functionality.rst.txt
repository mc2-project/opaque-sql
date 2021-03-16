.. _functionalities:

*************************
Supported functionalities
*************************

This section lists Opaque's supported functionalities, which is a subset of that of Spark SQL. Note that the syntax for these functionalities is the same as Spark SQL -- Opaque simply replaces the execution to work with encrypted data.

SQL interface
#############

Data types
**********

Out of the existing `Spark SQL types <https://spark.apache.org/docs/latest/sql-ref-datatypes.html>`_, Opaque supports

- All numeric types. ``DecimalType`` is supported via conversion into ``FloatType``
- ``StringType``
- ``BinaryType``
- ``BooleanType``
- ``TimestampTime``, ``DateType``
- ``ArrayType``, ``MapType``

Functions
*********

We currently support a subset of the Spark SQL functions, including both scalar and aggregate-like functions.

- Scalar functions: ``case``, ``cast``, ``concat``, ``contains``, ``if``, ``in``, ``like``, ``substring``, ``upper``
- Aggregate functions: ``average``, ``count``, ``first``, ``last``, ``max``, ``min``, ``sum``

UDFs are not supported directly, but one can :ref:`extend Opaque with additional functions <udf>` by writing it in C++.


Operators
*********

Opaque supports the core SQL operators:

- Projection (e.g., ``SELECT`` statements)
- Filter
- Global aggregation and grouping aggregation
- Order by, sort by
- All join types except: cross join, full outer join, existence join
- Limit

DataFrame interface
###################

TODO

.. _udf:

User-Defined Functions (UDFs)
#############################

To run a Spark SQL UDF within Opaque enclaves, first name it explicitly and define it in Scala, then reimplement it in C++ against Opaque's serialized row representation.

For example, suppose we wish to implement a UDF called ``dot``, which computes the dot product of two double arrays (``Array[Double]``). We [define it in Scala](src/main/scala/edu/berkeley/cs/rise/opaque/expressions/DotProduct.scala) in terms of the Breeze linear algebra library's implementation. We can then use it in a DataFrame query, such as `logistic regression <src/main/scala/edu/berkeley/cs/rise/opaque/benchmark/LogisticRegression.scala>`_.

Now we can port this UDF to Opaque as follows:

1. Define a corresponding expression using Opaque's expression serialization format by adding the following to [Expr.fbs](src/flatbuffers/Expr.fbs), which indicates that a DotProduct expression takes two inputs (the two double arrays):

   .. code-block:: protobuf
                   
                   table DotProduct {
                     left:Expr;
                     right:Expr;
                   }

   In the same file, add ``DotProduct`` to the list of expressions in ``ExprUnion``.

2. Implement the serialization logic from the Scala ``DotProduct`` UDF to the Opaque expression that we just defined. In ``Utils.flatbuffersSerializeExpression`` (from ``Utils.scala``), add a case for ``DotProduct`` as follows:

   .. code-block:: scala
                   
                   case (DotProduct(left, right), Seq(leftOffset, rightOffset)) =>
                     tuix.Expr.createExpr(
                       builder,
                       tuix.ExprUnion.DotProduct,
                       tuix.DotProduct.createDotProduct(
                         builder, leftOffset, rightOffset))


3. Finally, implement the UDF in C++. In ``FlatbuffersExpressionEvaluator#eval_helper`` (from ``ExpressionEvaluation.h``), add a case for ``tuix::ExprUnion_DotProduct``. Within that case, cast the expression to a ``tuix::DotProduct``, recursively evaluate the left and right children, perform the dot product computation on them, and construct a ``DoubleField`` containing the result.

   
