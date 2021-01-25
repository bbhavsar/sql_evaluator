# SQL Evaluator 

Author: Bankim Bhavsar (bankim.bhavsar@gmail.com)

Implemented SQL Evaluator in Java using the starter code provided.

C++ is my first choice language however there was no starter code in C++ and didn't
want to spend time implementing a bunch of Json serialization/deserializing code in C++.
Hence, chose to implement in Java.

Bulk of the implementation is in the new `QueryEvaluator.java` class.
It has 2 public function, the constructor and the `Evaluate()` function that works through the
steps of validating, computing cross product, filtering the rows, and projecting the requested
columns.
Minor updates to `Main.java` to invoke the `QueryEvaluator.java` class.

Implementation pre-computes data structures that helps with aliases in "from" clause
and "select" clause as it's required in a bunch of different places. See the in-line
comments in the `QueryEvaluator.java` class.

Cross product computation is simplistic O(n1 * n2 * n3 * .. nn) where m and n are number of
rows in the the tables n1, n2, n3, etc.

Compile the Java app using mvn compile

```bash
airtable_sql_evaluator_exercise_v26/starter-code/java$ mvn compile
```

Run the examples and verify

```bash
airtable_sql_evaluator_exercise_v26$ ./check starter-code/java/sql_evaluator -- examples examples/*.sql
Checking "examples/cities-1.sql"...
Checking "examples/cities-2.sql"...
Checking "examples/cities-3.sql"...
Checking "examples/error-1.sql"...
Checking "examples/error-2.sql"...
Checking "examples/error-3.sql"...
Checking "examples/simple-1.sql"...
Checking "examples/simple-2.sql"...
Checking "examples/simple-3.sql"...
Passed: 9/9
```

