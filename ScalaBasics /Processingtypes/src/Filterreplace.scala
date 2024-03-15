

object Filterreplace {
  
  def main(args:Array[String]):Unit={
    
    println("Filter,Replace in scala")
    
    val  list = List("Zeyobron_zeyo","zeyo_xwee","bron_ze")
    
    val repl = list.map(list =>list.replace("Zeyo","giggo"))
 
    val flatmap1 = repl.flatMap(rep1=>rep1.split('_'))
    
    println(flatmap1)
    
  
  }
}