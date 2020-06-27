package dedmp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DataFrame {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setAppName("myapp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val path = "C:\\Users\\Emran\\Desktop\\data\\npw\\eBay.csv"
    val ebayDF = sqlContext.read.option("header", true).option("inferSchema", true).csv(path)
      .toDF("auctionid", "bid", "bidtime", "bidder", "bidderrate", "openbid", "price")

    // ebayDF.show(5)  // shows top 5 records
    //ebayDF.show()  // by default shows top 20 records

    // ebayDF.collect().foreach(println) // shows all records
    //  ebayDF.printSchema()

    // selected columns:
    // ebayDF.select("auctionid", "price").show(10)

    // filter
    // ebayDF.filter("bidder =='pensri'").show()
    //println(ebayDF.filter("bidder =='pensri'").count())

    // ebayDF.filter("price == 355").show()

    // ebayDF.filter("bidder like '%1'").show()
    // println(ebayDF.filter("bidder like 'c%'").count())

    // filter chaining: ebayDF.filter("bidder like '%1' and price = 176.5").show()
    // ebayDF.filter("bidder like '%1' OR price = 176.5").show()
    // ebayDF.filter("price in (2500, 1250, 355)").collect().foreach(println)

    //ebayDF.filter("price NOT IN (2500, 1250, 355)").collect().foreach(println)

    // ebayDF.select("openbid").distinct().show()

    // ebayDF.groupBy("openbid").count().show()
    // ebayDF.groupBy("openbid").count().collect().foreach(println)
    // println("Total unique bid is: " + ebayDF.select("openbid").distinct().count())

    // ebayDF.groupBy("auctionid").count().show()
    // ebayDF.groupBy("auctionid").count().filter("count < 10").show()
    ebayDF.groupBy("auctionid").count().filter("count > 10 and count < 25").show()
  }

}
