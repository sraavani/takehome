import ucar.nc2._
import scala.collection.mutable.ArrayBuffer
import java.util.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

//get Variable Dew Point Temperature as DataFrame
case class DewPointTemperature(timeIndex : Integer, latIndex: Integer, longIndex: Integer, DewPointTemperature: Float)
//get Variable Air Temperature as DataFrame
case class AirTemperature(timeIndex : Integer, latIndex: Integer, longIndex: Integer, AirTemperature: Float)
//get Dimension Latitude as DataFrame
case class Latitude(latIndex : Long, Latitude : Float)
//get Dimension Longitude as DataFrame
case class Longitude(longIndex : Long, Longitude : Float)
//get Dimension Time as DataFrame
case class Time(timeIndex : Long, Time : Double)

//get .nc file from the S3 bucket raw folder 
val inputPath = "s3://awsbigwindllc/raw/air_temperature"
//save parquet file to S3 bucket processed folder
val outputPath = "s3://awsbigwindllc/processed/air_temperature"

//get Spark Context
val spark = SparkSession
.builder()
.appName("awshomeworknetCDFFile").getOrCreate()
val sc = spark.sparkContext

import spark.implicits._

//date format to convert epoch time
val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

//load netcdf file from the input path
val netcdfFile = sc.binaryFiles(inputPath).map(x => NetcdfFile.openInMemory("myfile", x._2.toArray))

//get the time dimension as a double array
val timeDF = netcdfFile.map(x => x.findVariable("time").read.copyTo1DJavaArray.asInstanceOf[Array[Double]]).flatMap(x => x).zipWithIndex.map( x => Time(x._2, x._1)).toDF //get the temperature variable as a 3 dimentional array and get the size of each dimension

//get the size of the time, latitude, and longitude dimensions for the temperature varaible
val tempAir = netcdfFile.map( x => {
val shape = x.findVariable("air_temperature").getShape()
(shape(0),shape(1),shape(2), x.read("air_temperature", true).copyToNDJavaArray.asInstanceOf[Array[Array[Array[Float]]]]) })

//get the size of the time, latitude, and longitude dimensions for the temperature varaible
val tempDP = netcdfFile.map( x => {
val shape = x.findVariable("dew_point_temperature").getShape()
(shape(0),shape(1),shape(2), x.read("dew_point_temperature", true).copyToNDJavaArray.asInstanceOf[Array[Array[Array[Float]]]]) })

//get Air Temperature in reference to time, lat and long coordinates
val tempDF = tempAir.map( x => {
    var newTempArray = new ArrayBuffer[AirTemperature]
    for (timeIndex <- 0 until x._1){
        for (latIndex <- 0 until x._2){ 
            for (longIndex <- 0 until x._3){
                newTempArray.append(AirTemperature(timeIndex,latIndex, longIndex, x._4((timeIndex ))((latIndex ))((longIndex ))-273)) //convert to Celsius
            }
} }
newTempArray }).flatMap(x => x).toDF

//get Dew Point Temperature in reference to time, lat and long coordinates
val tempDPDF = tempDP.map( x => {
    var newTempArray = new ArrayBuffer[DewPointTemperature]
    for (timeIndex <- 0 until x._1){
        for (latIndex <- 0 until x._2){ 
            for (longIndex <- 0 until x._3){
                newTempArray.append(DewPointTemperature(timeIndex,latIndex, longIndex, x._4((timeIndex ))((latIndex ))((longIndex ))-273)) //convert to Celsius
            }
} }
newTempArray }).flatMap(x => x).toDF

//get lat, long from attributes
val latDF = netcdfFile.map(x => x.findVariable("grid_latitude").read.copyTo1DJavaArray.asInstanceOf[Array[Float]]).flatMap(x => x).zipWithIndex.map( x => Latitude(x._2, x._1)).toDF 
val longDF = netcdfFile.map(x => x.findVariable("grid_longitude").read.copyTo1DJavaArray.asInstanceOf[Array[Float]]).flatMap(x => x).zipWithIndex.map( x => Longitude(x._2,x._1)).toDF

val airDF = tempDF.join(timeDF, Seq("timeIndex"), "left_outer").join(latDF, Seq("latIndex"), "left_outer").join(longDF, Seq("longIndex"), "left_outer")
val dpDF = tempDPDF.join(timeDF, Seq("timeIndex"), "left_outer").join(latDF, Seq("latIndex"), "left_outer").join(longDF, Seq("longIndex"), "left_outer")
val finalDF = airDF.join(dpDF, Seq("timeIndex", "latIndex", "longIndex", "Time", "Latitude", "Longitude"))
  
//finalDF.show()

finalDF.coalesce(4).write.mode("append").parquet(outputPath)
