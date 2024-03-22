import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.Row._



object SelectExpr {
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
    
    val selexprdf = xmldf.selectExpr("id",
                                     "split(tdate,'-')[2] as tdate",
                                     "amount",
                                     "category",
                                     "UPPER(product)",
                                     "spendby"
                                     )
                                     
                                     
     selexprdf.show()
    
}
}