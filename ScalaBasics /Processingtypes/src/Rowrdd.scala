import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._



object Rowrdd {
  
  
  def main(args:Array[String]):Unit={
    
    System.setProperty("hadoop.home.dir","D:\\hadoop")
    
    val conf = new SparkConf().setAppName("DSL").setMaster("local[*]").set("spark.driver.allowMultipleContext","true")
    val sc = new SparkContext(conf)
    
    sc.setLogLevel("ERROR")
    
    
    
    val spark=SparkSession.builder.getOrCreate()
    
    import spark.implicits._
    
    val data= sc.textFile("file:///D:/Big_Data/Files/dr.txt")
    
    data.foreach(println)
    println()
    println()
    println()
    
    
    //do mapslipt
    
    val split= data.map(x=>x.split(','))
    println(split)
    
    
    //schemardd(row based )
    val schemardd = split.map(x=> Row(x(0),x(1),x(2),x(3),x(4),x(5)))
    
    ///Filter only gymnastic data & this is row rdd
    println("filter gymnastic data")
    val filt = schemardd.filter(x=>x(4).toString().contains("Gymnastics"))
    filt.foreach(println)
   
    val colschema = StructType(Array(
                                     StructField("first",StringType,true),
                                     StructField("second",StringType,true),
                                     StructField("third",StringType,true),
                                     StructField("fourth",StringType,true),
                                     StructField("fifth",StringType,true),
                                     StructField("sixth",StringType,true)
                                     ))
   val rdf = spark.createDataFrame(filt, colschema)
   rdf.show()
   
   rdf.write.parquet("file:///D:/Big_Data/Files/rowrdd")
   
}
}