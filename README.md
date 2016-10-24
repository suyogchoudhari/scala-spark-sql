# scala-spark-sql
scala spark sql example

Problem statement
----------

**There are two datasets:**

*User information (id, email, language, location)* - provided sample txt file in code transactions_test.txt

*Transaction information (transaction-id, product-id, user-id, purchase-amount, item-description)* - provided sample txt file in code users_test.txt

Given these datasets, I want to find the number of unique locations in which each product has been sold. To do that, I need to join the two datasets together.

**Sample Data**
<code><p>
users<br>
1	matthew@test.com	EN	US<br>
2	matthew@test2.com	EN	GB<br>
3	matthew@test3.com	FR	FR<br>
</code></p>
<code><p>
transactions
1	1	1	300	a jumper
2	1	2	300	a jumper
3	1	2	300	a jumper
4	2	3	100	a rubber chicken
5	1	3	300	a jumper
</code></p>
Reference blog: http://beekeeperdata.com/posts/hadoop/2015/12/14/spark-scala-tutorial.html
