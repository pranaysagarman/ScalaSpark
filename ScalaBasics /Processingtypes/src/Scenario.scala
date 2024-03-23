
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row._

object Scenario {
  
  def main(args:Array[String]):Unit={
    
   System.setProperty("hadoop.home.dir","D:\\hadoop")
    
    val conf = new SparkConf().setAppName("Read").setMaster("local[*]").set("spark.driver.allowMultipleContext","true")
    val sc = new SparkContext(conf)
    
    sc.setLogLevel("ERROR")
    
    
    
    val spark=SparkSession.builder.getOrCreate()
    
    import spark.implicits._
    
    val sourcedf = spark.read
                     .format("csv")
                     .option("header","true")
                     .load("file:///D:/Big_Data/Files/Source.csv")
                     .withColumnRenamed("name","sname")
    sourcedf.show()
    
     val targetdf = spark.read
                      .format("csv")
                     .option("header","true")
                      //chanage according to ur path
                     .load("file:///D:/Big_Data/Files/Target.csv")
                     .withColumnRenamed("Sname","tname")
    targetdf.show()
    
    
    val joindf = sourcedf.join(targetdf,Seq("id"),"full")
    joindf.show()
    
    val scendf = joindf.withColumn("Comments",expr("""case 
                                                    when sname=tname then 'match'
                                                    when sname is null then 'New value in Target'
                                                    when tname is null then 'New value in Source'
                                                    else 'mismatch'
                                                    end"""))
    scendf.show()
    
    val oudf = scendf.filter(!(col("Comments")==="match"))
    oudf.show()
    
    val outdf = oudf.drop("sname","tname")
    outdf.show()
    //output
    
}
}