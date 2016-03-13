import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.SQLContext

// represents a single record from movie reviews file
case class Review(productId: String, userId: String, profileName: String, helpfulness: String, score: String, time: String, summary: String, text: String)

object Movies {
	def main (args: Array[String]) {
		
		// initialise spark context
		val conf = new SparkConf().setAppName("Movie Reviews")
		val sc = new SparkContext(conf)

		// configure record delimiter as double new line (default is new line)
		val mConf = new Configuration(sc.hadoopConfiguration)
		mConf.set("textinputformat.record.delimiter", "\n\n")

		// read movie reviews file
		val reviews = sc.newAPIHadoopFile("moviesample.txt", classOf[TextInputFormat], classOf[LongWritable], classOf[Text], mConf).map(_._2.toString)
		
		// initialise sql context
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._

		// get records using new line as field delimiter
		val records = reviews.map(record => record.split("\n"))

		// remove tag from field (eg "product/productId: ")
		val cleanRecords = records.map(record => record.map(field => field.split(": ")(1)))

		// create a data frame
		val data = cleanRecords.map(r => Review(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7))).toDF()

		// register data frame as a temporary sql table
		data.registerTempTable("data")

		// query table and print result to test
		val sample = sqlContext.sql("SELECT productId, summary from data")
		sample.foreach(println)
	}
}