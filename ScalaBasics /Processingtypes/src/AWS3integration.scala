
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row._


object AWS3integration {
  
  def main(args:Array[String]):Unit={
    
   System.setProperty("hadoop.home.dir","D:\\hadoop")
    
    val conf = new SparkConf().setAppName("Read").setMaster("local[*]").set("spark.driver.allowMultipleContext","true")
    val sc = new SparkContext(conf)
    
    sc.setLogLevel("ERROR")
    
    
    
    val spark=SparkSession.builder.getOrCreate()
    
    import spark.implicits._
    
    val xmldf = spark.read
                    .format("json")
                    .option("fs.s3a.access.key","**************")
                    .option("fs.s3a.secret.key","(**************")
                    .load("s3a://s3databuck/devices.json")
    
    xmldf.show()
    
   
}
}