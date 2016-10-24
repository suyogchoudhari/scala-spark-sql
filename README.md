# scala-spark-sql
scala spark sql example

Problem statement
----------

**There are two datasets:**

*User information (id, email, language, location)* - provided sample txt file in code transactions_test.txt

*Transaction information (transaction-id, product-id, user-id, purchase-amount, item-description)* - provided sample txt file in code users_test.txt

Given these datasets, I want to find the number of unique locations in which each product has been sold. To do that, I need to join the two datasets together.

Reference blog: http://beekeeperdata.com/posts/hadoop/2015/12/14/spark-scala-tutorial.html


How to run
----------

*mvn clean package -e*

*spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class com.spsc.spsctest.ScalaSparkSqlTest --master local ./target/spsctest-0.0.1-SNAPSHOT.jar transactions_test.txt users_test.txt*
