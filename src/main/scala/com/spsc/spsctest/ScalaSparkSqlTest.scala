package com.spsc.spsctest

import org.apache.spark.sql.SparkSession

/**
 * @author ${suyog}
 */

case class Transaction(transaction_id: Long, product_id: Long, user_id: Long, purchase_amount: Long, item_description: String)
case class User(id: Long, email: String, language: String, location: String)

class ScalaSparkSqlTest(spark: SparkSession) extends java.io.Serializable  {
  val minPartitions = 1;
  
  import spark.implicits._
  
  def testSparkSql(transactions: String, users: String) : Unit = {
    
    //transactions data frame
    val transactionsDF = spark.sparkContext
    .textFile(transactions, minPartitions)
    .map(_.split("\t"))
    .map(attributes => Transaction(attributes(0).toInt, attributes(1).toInt, attributes(2).toInt, attributes(3).toInt, attributes(4)))
    .toDF()
    
    // Register the DataFrame as a temporary view
    transactionsDF.createOrReplaceTempView("transaction")
    
    //users data frame
    val usersDF = spark.sparkContext
    .textFile(users, minPartitions)
    .map(_.split("\t"))
    .map(attributes => User(attributes(0).toInt, attributes(1), attributes(2), attributes(3)))
    .toDF()
    
    // Register the DataFrame as a temporary view
    usersDF.createOrReplaceTempView("user")
    
    spark.sql("select distinct product_id, count(distinct location) from transaction t LEFT OUTER JOIN user u ON u.id = t.user_id GROUP BY t.product_id").show()
  }
  
}

object ScalaSparkSqlTest {
    def main(arg: Array[String]) {
      
      //transactions and users file path
      val transactions = arg(0)
      val users = arg(1)
      
      //spark session
      val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    
      val testObject = new ScalaSparkSqlTest(spark)
      testObject.testSparkSql(transactions,users)
      
      spark.stop()
    }
    
}