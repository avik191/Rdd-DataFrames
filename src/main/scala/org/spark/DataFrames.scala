package org.spark

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window

object DataFrames {
  
  case class Product(itemId : Int, category : String, productNumber : Int, name : String)
  case class Customers(customerId : Int, name : String)
  case class Sales(transactionId : Int, customerId : Int, itemId : Int, amount : Option[Double], date : String)
  
  def getCustomerRdd(line : String) = {
    val temp = line.split(',')
    Customers(temp(0).toInt,temp(1))
  }
  
  def getSalesRdd(line : String) = {
    val temp = line.split(',')
    var amount : Option[Double]= Some(0)
    
    if(temp(3) == "" || temp(3) == null) amount = null
    else amount = Some(temp(3).toDouble)
    
    Sales(temp(0).toInt,temp(1).toInt,temp(2).toInt,amount,temp(4))
  }
  
  def getProductRdd(line : String) = {
    val temp = line.split(',')
    Product(temp(0).toInt,temp(1),temp(2).toInt,temp(3))
  }
  
  def main(args: Array[String]): Unit = {
    
      Logger.getLogger("org").setLevel(Level.ERROR)
    
      System.setProperty("hadoop.home.dir", "G://sparkResource//winutls//");
      val conf = new SparkConf().

      setAppName("").
      setMaster("local").
      set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts","true");
      
      val sparkContext = new SparkContext(conf);
      val sqlContext = new SQLContext(sparkContext)
      
      import sqlContext.implicits._
      import org.apache.spark.sql.functions._

      val products_data = sparkContext.textFile("G://BigData//interview_questions//data//products.csv")
      val customers_data = sparkContext.textFile("G://BigData//interview_questions//data//customers.csv")
      val sales_data = sparkContext.textFile("G://BigData//interview_questions//data//sales.csv")

      val productSchema = products_data.first()
      val productsRdd = products_data.filter(line => line != productSchema).map(getProductRdd)
      //productsRdd.collect().foreach(println)
      
      val customerSchema = customers_data.first()
      val customerRdd = customers_data.filter(line => line != customerSchema).map(getCustomerRdd)
      
      val SalesSchema = sales_data.first()
      val SalesRdd = sales_data.filter(line => line != SalesSchema).map(getSalesRdd)
      
      val products_Df = productsRdd.toDF()
      val customers_Df = customerRdd.toDF()
      val sales_Df = SalesRdd.toDF()
      //sales_Df.show()
      
      // Task 1 : Count of each files
      println(s"Products.csv - ${products_Df.count()}")
      println(s"Customers.csv - ${customers_Df.count()}")
      println(s"Sales.csv - ${sales_Df.count()}")
      
      // Task 2 : Handle Nulls. Replace null values with mean of amount
      val row = sales_Df.select(sales_Df("amount")).agg(mean("amount")).collect()
      val mean_value = row(0)(0) // first row,first column
      val x = mean_value.toString().toDouble // for rounding of the mean value to two decimal places
      val round = Math.round(x*100.0)/100.0
     
      val updatedSales_Df = sales_Df.select("*").
            withColumn("amount", when(!isnull(sales_Df("amount")),sales_Df("amount")).otherwise(round))
      
      updatedSales_Df.show()
      
      // Task 3 : Join all the above files on customerId and itemId.
      val customer_sales_join = customers_Df.join(updatedSales_Df,"customerId").
                          withColumn("customerName",customers_Df("name")).drop(customers_Df("name"))
                          
      val customer_product_sales_join = products_Df.join(customer_sales_join,"itemId").
                          withColumn("productName",products_Df("name")).drop(products_Df("name"))
                          
      customer_product_sales_join.show()
      
      // Task 4 : Add a Sequential Row ID
      val order =  Window.orderBy("itemId")                    
      val joined_Table = customer_product_sales_join.withColumn("id",row_number().over(order))
     
      joined_Table.show()
      
      // Task 5 : Sales By Week
      val final_table = joined_Table.withColumn("date", joined_Table("date").cast("date"))
      val table = final_table.withColumn("week_no", weekofyear(final_table("date")))
                                                                    
      table.groupBy(table("week_no")).sum("amount").show()
      
      // Task 6 : Sales with 5% Discount
      val discounted_table = table.withColumn("discounted_amount",table("amount")*0.05)
      discounted_table.show()
      
  }
  
}