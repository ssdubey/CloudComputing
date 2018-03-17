import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("wordCount").setMaster("local[1]");
      val sc = new SparkContext(conf)
      
      var values: RDD[String] = sc.emptyRDD
      val input =  sc.wholeTextFiles("/home/shashank/myFolder/cloudWorkspace/StreamingHelloWorld/src/StreamingHelloWorld/src/*/")
     
      values = values++input.values
      values = values.coalesce(1)
      values = values.flatMap { word => word.split("\n") }
      val wordcount = values.map { w => (w, 1) }
      val wc = wordcount.reduceByKey(_+_)
      wc.saveAsTextFile("/home/shashank/myFolder/cloudWorkspace/WordCountScala/src/output.txt")
}
}