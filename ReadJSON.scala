package sparkcodes

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ReadJSON {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    
    var cnf = new SparkConf().setAppName("readjson").setMaster("local[*]")

    val sc = new SparkContext(cnf)

    val sqlc = new SQLContext(sc)
    
    var path = "C:\\SparkScala\\ScalaSparkLearning\\data\\PoliceData.csv";
    val b =sqlc.read.json(path)
    b.collect.foreach(println)

  }
}
