//Use case 1 using 
//1. use split by '-'
//2.flat map
//3.

object RDDDF {
  
  def main(args:Array[String]):Unit={
    
    println("  ")
    println("Input Raw List ")
    println
    
    val lis =List("BigData-Spark-Hive",
                  "Spark-Hadoop-Hive",
                  "Sqoop-Hive-Spark",
                  "Sqoop-BD-Hive")
    lis.foreach(println)
    
    println("Output list")
    println("Tech-->BigData Trainer ->PS",
        "Tech-->Spark Trainer->PS",
        "Tech-->Hive Trainer -->PS",
        "Tech-->Hadoop Trainer -->PS",
        "Tech-->BDTrainer -->PS")
        
     println("Step 1")
     val flatdata = lis.flatMap(x=>x.split('-'))
     println(flatdata)
     
     
     println("Distinct List")
     val lis1 = flatdata.distinct
     println(lis1)
    
    println("-----Map--------")
    val finalist =lis1.map(x=> "Tech-->" +x+"Trainer -->PS")
    finalist.foreach(println)
     
     
  }
}