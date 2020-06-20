package dataframedemo
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object DFDemo3 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutils")
    val conf = new SparkConf().setAppName("testdf").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

     val df_mysql = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/world").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "city").option("user", "root").option("password", "abc123").load()

    df_mysql.show()

  }
}
