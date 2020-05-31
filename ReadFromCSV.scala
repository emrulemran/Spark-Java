package sparkcodes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object ReadFromCSV {
  def main(args: Array[String]): Unit = {
    // hadoop home directory path:
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    var myconfig = new SparkConf().setAppName("rafi").setMaster("local[*]")
    var sc = new SparkContext(myconfig)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    var path = "C:\\Users\\Desktop\\salesid.csv"
    val mydf = spark.read.option("header", "true").option("inferSchema", "true").csv(path);

    //mydf.show()

    //    mydf.printSchema()

    // mydf.groupBy("ptype").count().show()

    //mydf.select("id", "ptype").show();

    //    var new_df = mydf.where(mydf("id")>2).select("id")
    //    new_df.show()

    var name = mydf.where(mydf("name") === "Betina" && mydf("country") === "United States").select("name", "country")
    name.show

    // Register the DataFrame as a SQL temporary view
    mydf.createOrReplaceTempView("sales")
    //    val sqlDF = spark.sql("SELECT id, name, city FROM sales where name='Betina'")
    //    sqlDF.show()

  }
}
