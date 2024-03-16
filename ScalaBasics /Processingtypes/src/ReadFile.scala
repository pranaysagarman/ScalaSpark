import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object ReadFile {
  
  def main(args:Array[String]):Unit={
    
   val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
   
   val sc = new SparkContext(conf)
   
   sc.setLogLevel("Error")
   //System.setProperty("hadoop.home.dir","D:/Hadoop/bin/winutils.exe")
   
   //chnage the file path name based on ur local
   val readfile =  sc.textFile("file:///D:/Big_Data/Files/usdata.csv")
   
   
   //taking only five values from the list
   readfile.take(5).foreach(println)
   
   
   //iterate each row from the data which has value greatert han 200
   //Flatten the data with comma delimiter & remove hyphnes
   //concat "Zeyo "from resultant
   
   //using filter to get the requirement
   
   val len = readfile.filter(x=>x.length >200)
   
   len.foreach(println)
  
   //flatten data
   
   
   println("==============Flatten data===================")
   val faltdata = len.flatMap(x=>x.split(','))
   
   faltdata.foreach(println)
   
   
   println("=================replace the data===========")
   //replace the data
    val rep = faltdata.map(x => x.replace("-",""))
    rep.foreach(println)
    
    //concat the data
    println("====================Concat=============")
    val cocn = rep.map(x=>x.concat("demo"))
    cocn.foreach(println)
   
    
    //writeit to  the file 
    
    cocn.coalesce(1).saveAsTextFile("file:///D:/Big_Data/Files/Usdatawrite")
   
  }
}