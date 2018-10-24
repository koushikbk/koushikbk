package Telco.Code

import java.io.File

import Telco.Methods.CaseClass._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object Service_based_churn {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("").master("local").getOrCreate()
    var sc = sparkSession.sparkContext
    val config = ConfigFactory.parseFile(new File(args(0)))
    val filepath = config.getString("filepath")
    val rdd = sc.textFile(filepath.toString)
    import sparkSession.implicits._
    var Data = rdd.map(line => line.split(",")).map(row =>
      try {
        Telco(row(0),
          row(1),
          row(2).toInt,
          row(3),
          row(4),
          row(5).toInt,
          row(6),
          row(7),
          row(8),
          row(9),
          row(10),
          row(11),
          row(12),
          row(13),
          row(14),
          row(15),
          row(16),
          row(17),
          row(18).toFloat,
          row(19).toFloat,
          row(20))
      }
      catch {
        case exp: IllegalArgumentException => {
          Telco(row(0),
            "",
            -999,
            "",
            "",
            -999,
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            -999,
            -999,
            "")
        }
      }
    ).toDF()
    Data.createOrReplaceTempView("telecom")
    var Query = sparkSession.sql("SELECT " +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "PhoneService = 'Yes' THEN PhoneService END) " +
      "AS churned_with_phoneservice,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "MultipleLines = 'Yes' THEN MultipleLines END) " +
      "AS churned_with_MultipleLines,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "InternetService = 'Yes' THEN InternetService END) " +
      "AS churned_with_InternetService,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "OnlineSecurity = 'Yes' THEN OnlineSecurity END) " +
      "AS churned_with_OnlineSecurity,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "OnlineBackup = 'Yes' THEN OnlineBackup END) " +
      "AS churned_with_OnlineBackup,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "DeviceProtection = 'Yes' THEN DeviceProtection END) " +
      "AS churned_with_DeviceProtection,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "TechSupport = 'Yes' THEN TechSupport END) " +
      "AS churned_with_TechSupport,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "StreamingTV = 'Yes' THEN StreamingTV END) " +
      "AS churned_with_StreamingTV,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "StreamingMovies = 'Yes' THEN StreamingMovies END) " +
      "AS churned_with_StreamingMovies," +
      "count(CASE WHEN Churn = 'Yes' AND " +
      "PhoneService = 'No' THEN PhoneService END) " +
      "AS churned_withOut_phoneservice,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n " +
      "MultipleLines = 'No' THEN MultipleLines END) " +
      "AS churned_withOut_MultipleLines,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "InternetService = 'No' THEN InternetService END) " +
      "AS churned_withOut_InternetService,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "OnlineSecurity = 'No' THEN OnlineSecurity END) " +
      "AS churned_withOut_OnlineSecurity,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "OnlineBackup = 'No' THEN OnlineBackup END) " +
      "AS churned_withOut_OnlineBackup,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "DeviceProtection = 'No' THEN DeviceProtection END) " +
      "AS churned_withOut_DeviceProtection,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "TechSupport = 'No' THEN TechSupport END) " +
      "AS churned_withOut_TechSupport,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "StreamingTV = 'No' THEN StreamingTV END) " +
      "AS churned_withOut_StreamingTV,\n" +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "StreamingMovies = 'No' THEN StreamingMovies END) " +
      "AS churned_withOut_StreamingMovies," +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "MultipleLines = 'No phone service' THEN MultipleLines END) " +
      "AS churned_without_Phone_service_MultipleLines\n," +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "InternetService = 'Fiber optic' THEN InternetService END) " +
      "AS churned_with_FiberOptic\n," +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "OnlineSecurity = 'No internet service' THEN OnlineSecurity END) " +
      "AS churned_with_No_internet_service_OnlineSecurity\n," +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "OnlineBackup = 'No internet service' THEN OnlineBackup END) " +
      "AS churned_with_No_internet_service_OnlineBackup\n," +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "DeviceProtection = 'No internet service' THEN DeviceProtection END) " +
      "AS churned_with_No_internet_service_DeviceProtection\n," +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "TechSupport = 'No internet service' THEN TechSupport END) " +
      "AS churned_with_No_internet_service_TechSupport\n," +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "StreamingTV = 'No internet service' THEN StreamingTV END) " +
      "AS churned_with_No_internet_service_StreamingTV\n," +
      "count(CASE WHEN Churn = 'Yes' AND \n" +
      "StreamingMovies = 'No internet service' THEN StreamingMovies END) " +
      "AS churned_with_No_internet_service_StreamingMovies\nfrom " +
      "telecom").coalesce(1).write.option("header", "true").csv(config.getString("outputpath")+"_Service_based")
  }
}
