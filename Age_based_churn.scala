package Telco.Code

import java.io.File

import Telco.Methods.CaseClass.Telco
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object Age_based_churn {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("").master("local").getOrCreate()
    var sc = sparkSession.sparkContext
    val config = ConfigFactory.parseFile(new File(args(0)))
    val filepath = config.getString("filepath")
    val rdd = sc.textFile(filepath.toString)
    import sparkSession.implicits._
    var Data = rdd.map(line => line.split(",")).
      map(row => try {
        Telco(row(0), row(1), row(2).toInt, row(3), row(4), row(5).toInt, row(6), row(7), row(8), row(9), row(10), row(11), row(12), row(13), row(14),
          row(15), row(16), row(17), row(18).toFloat, row(19).toFloat, row(20))
      }
      catch {
        case ex@(_: NumberFormatException | _: IllegalArgumentException) => {
          Telco(row(0), row(1), -999, row(3), row(4), -999, row(6), row(7), row(8), row(9), row(10), row(11), row(12), row(13), row(14),
            row(15), row(16), row(17), -999, -999, row(20))
        }
      }).toDF
    Data.createOrReplaceTempView("telecom")
    val Query = sparkSession.sql("SELECT SeniorCitizen,count(SeniorCitizen) FROM telecom WHERE Churn='Yes' GROUP BY SeniorCitizen").show()
    //  .coalesce(1).write.option("header", "true").csv(config.getString("outputpath")+"_Age_based")
  }
}
