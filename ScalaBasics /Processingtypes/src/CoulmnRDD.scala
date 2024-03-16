import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object CoulmnRDD {
  
  
  case class schema(firstcol:String,secondcol:String,thirdcol:String,fourthcol:String,fifthcol:String,sixthcol:String)
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    
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
    val schemardd = split.map(x => schema(x(0),x(1),x(2),x(3),x(4),x(5)))
    
    ///Filter only gymnastic data & this is row rdd
    println("filter gymnastic data")
    val filt = schemardd.filter(x=>x.fifthcol.contains("Gymnastics"))
    filt.foreach(println)
   
    //convert it to df
    val df = filt.toDF()
    println(df)
    
    
  }
  
}