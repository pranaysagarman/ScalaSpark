
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row._

object Task {
  

def main(args:Array[String]):Unit={
    
   System.setProperty("hadoop.home.dir","D:\\hadoop")
    
    val conf = new SparkConf().setAppName("Read").setMaster("local[*]").set("spark.driver.allowMultipleContext","true")
    val sc = new SparkContext(conf)
    
    sc.setLogLevel("ERROR")
    
    
    
    val spark=SparkSession.builder.getOrCreate()
    
    import spark.implicits._
    
    val xmldf = spark.read
                     .format("csv")
                     .option("header","true")
                     .load("file:///D:/Big_Data/Files/dt.txt")
    xmldf.show()
    
    
    val procdf =xmldf
                .filter(col("category") === "Exercise" )
                .withColumn("catpro",expr("concat(upper(category),'-',product)"))
                .withColumn("tdate",expr("split(tdate,'-')[2] as year"))
                .withColumnRenamed("tdate","year")
                .withColumn("status", expr("case when spendby ='cash' then 1 else 0 end"))
                 
    
     procdf.show()
}
}