import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]) {
    //  val inputFile = "/home/shashank/myFolder/cloudWorkspace/StreamingHelloWorld/src/StreamingHelloWorld/src/"//args(0)
    //  val outputFile = args(1)
      val conf = new SparkConf().setAppName("wordCount").setMaster("local[1]");
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      
      println("sfds")
      //-------------------------------
      
      var values: RDD[String] = sc.emptyRDD
      val input =  sc.wholeTextFiles("/home/shashank/myFolder/cloudWorkspace/StreamingHelloWorld/src/StreamingHelloWorld/src/*/")
     
      //input.values.collect().foreach(x=>println(x))
      
      values = values++input.values
      values = values.coalesce(1)
      values = values.flatMap { word => word.split("\n") }
      val wordcount = values.map { w => (w, 1) }
      val wc = wordcount.reduceByKey(_+_)
      wc.saveAsTextFile("/home/shashank/myFolder/cloudWorkspace/WordCountScala/src/output.txt")
      //-------------------------------
      
      
      /* val input =  sc.textFile(inputFile)
       
      // Split up into words.
      val words = input.flatMap(line => line.split(" ")).filter { x => (!x.isEmpty())}
      // Transform into word and count.
      
      val wordcount = words.map { w => (w,1) }
      val wc = wordcount.reduceByKey(_+_)
      
      wc.saveAsTextFile("/home/shashank/myFolder/cloudWorkspace/WordCountScala/src/output.txt")
     */
}
}