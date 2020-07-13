/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Test
 * Author: yanglan88
 * Date: 2020/6/23 23:12
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/23
 * @since 1.0.0
 */
package com.qf.dmp.etl

import java.util.Properties

import com.qf.dmp.utils.CommonUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Test {
    def main(args: Array[String]): Unit = {

        if (args == null || args.length != 2){
            println("Usage : <input> <output>")
            System.exit(-1)
        }

        val Array(input , output) = args

        val properties = new Properties()
        properties.load(Test.getClass.getClassLoader.getResourceAsStream("spark.properties"))

        val spark: SparkSession = SparkSession
            .builder()
            .appName(Test.getClass.getName)
            .master("local[*]")
            .getOrCreate()
        spark.sqlContext.setConf(properties)

        val rdd: RDD[String] = spark.sparkContext.textFile(input)

        val rddRow: RDD[Row] = rdd.map(_.split(",",-1)).filter(_.length >= 85).map(arr => {
            Row(
                arr(0),
                CommonUtils.toInt(arr(1)),
                CommonUtils.toInt(arr(2)),
                CommonUtils.toInt(arr(3)),
                CommonUtils.toInt(arr(4)),
                arr(5),
                arr(6),
                CommonUtils.toInt(arr(7)),
                CommonUtils.toInt(arr(8)),
                CommonUtils.toDouble(arr(9)),
                CommonUtils.toDouble(arr(10)),
                arr(11),
                arr(12),
                arr(13),
                arr(14),
                arr(15),
                arr(16),
                CommonUtils.toInt(arr(17)),
                arr(18),
                arr(19),
                CommonUtils.toInt(arr(20)),
                CommonUtils.toInt(arr(21)),
                arr(22),
                arr(23),
                arr(24),
                arr(25),
                CommonUtils.toInt(arr(26)),
                arr(27),
                CommonUtils.toInt(arr(28)),
                arr(29),
                CommonUtils.toInt(arr(30)),
                CommonUtils.toInt(arr(31)),
                CommonUtils.toInt(arr(32)),
                arr(33),
                CommonUtils.toInt(arr(34)),
                CommonUtils.toInt(arr(35)),
                CommonUtils.toInt(arr(36)),
                arr(37),
                CommonUtils.toInt(arr(38)),
                CommonUtils.toInt(arr(39)),
                CommonUtils.toDouble(arr(40)),
                CommonUtils.toDouble(arr(41)),
                CommonUtils.toInt(arr(42)),
                arr(43),
                CommonUtils.toDouble(arr(44)),
                CommonUtils.toDouble(arr(45)),
                arr(46),
                arr(47),
                arr(48),
                arr(49),
                arr(50),
                arr(51),
                arr(52),
                arr(53),
                arr(54),
                arr(55),
                arr(56),
                CommonUtils.toInt(arr(57)),
                CommonUtils.toDouble(arr(58)),
                CommonUtils.toInt(arr(59)),
                CommonUtils.toInt(arr(60)),
                arr(61),
                arr(62),
                arr(63),
                arr(64),
                arr(65),
                arr(66),
                arr(67),
                arr(68),
                arr(69),
                arr(70),
                arr(71),
                arr(72),
                CommonUtils.toInt(arr(73)),
                CommonUtils.toDouble(arr(74)),
                CommonUtils.toDouble(arr(75)),
                CommonUtils.toDouble(arr(76)),
                CommonUtils.toDouble(arr(77)),
                CommonUtils.toDouble(arr(78)),
                arr(79),
                arr(80),
                arr(81),
                arr(82),
                arr(83),
                CommonUtils.toInt(arr(84))
            )
        })

        val structType: StructType = StructType(Seq(
            StructField("sessionid", DataTypes.StringType),
            StructField("advertisersid", IntegerType),
            StructField("adorderid", IntegerType),
            StructField("adcreativeid", IntegerType),
            StructField("adplatformproviderid", IntegerType),
            StructField("sdkversion", StringType),
            StructField("adplatformkey", StringType),
            StructField("putinmodeltype", IntegerType),
            StructField("requestmode", IntegerType),
            StructField("adprice", DoubleType),
            StructField("adppprice", DoubleType),
            StructField("requestdate", StringType),
            StructField("ip", StringType),
            StructField("appid", StringType),
            StructField("appname", StringType),
            StructField("uuid", StringType),
            StructField("device", StringType),
            StructField("client", IntegerType),
            StructField("osversion", StringType),
            StructField("density", StringType),
            StructField("pw", IntegerType),
            StructField("ph", IntegerType),
            StructField("long", StringType),
            StructField("lat", StringType),
            StructField("provincename", StringType),
            StructField("cityname", StringType),
            StructField("ispid", IntegerType),
            StructField("ispname", StringType),
            StructField("networkmannerid", IntegerType),
            StructField("networkmannername", StringType),
            StructField("iseffective", IntegerType),
            StructField("isbilling", IntegerType),
            StructField("adspacetype", IntegerType),
            StructField("adspacetypename", StringType),
            StructField("devicetype", IntegerType),
            StructField("processnode", IntegerType),
            StructField("apptype", IntegerType),
            StructField("district", StringType),
            StructField("paymode", IntegerType),
            StructField("isbid", IntegerType),
            StructField("bidprice", DoubleType),
            StructField("winprice", DoubleType),
            StructField("iswin", IntegerType),
            StructField("cur", StringType),
            StructField("rate", DoubleType),
            StructField("cnywinprice", DoubleType),
            StructField("imei", StringType),
            StructField("mac", StringType),
            StructField("idfa", StringType),
            StructField("openudid", StringType),
            StructField("androidid", StringType),
            StructField("rtbprovince", StringType),
            StructField("rtbcity", StringType),
            StructField("rtbdistrict", StringType),
            StructField("rtbstreet", StringType),
            StructField("storeurl", StringType),
            StructField("realip", StringType),
            StructField("isqualityapp", IntegerType),
            StructField("bidfloor", DoubleType),
            StructField("aw", IntegerType),
            StructField("ah", IntegerType),
            StructField("imeimd5", StringType),
            StructField("macmd5", StringType),
            StructField("idfamd5", StringType),
            StructField("openudidmd5", StringType),
            StructField("androididmd5", StringType),
            StructField("imeisha1", StringType),
            StructField("macsha1", StringType),
            StructField("idfasha1", StringType),
            StructField("openudidsha1", StringType),
            StructField("androididsha1", StringType),
            StructField("uuidunknow", StringType),
            StructField("userid", StringType),
            StructField("iptype", IntegerType),
            StructField("initbidprice", DoubleType),
            StructField("adpayment", DoubleType),
            StructField("agentrate", DoubleType),
            StructField("lomarkrate", DoubleType),
            StructField("adxrate", DoubleType),
            StructField("title", StringType),
            StructField("keywords", StringType),
            StructField("tagid", StringType),
            StructField("callbackdate", StringType),
            StructField("channelid", StringType),
            StructField("mediatype", IntegerType)
        ))

        val df = spark.createDataFrame(rddRow, structType)

        df.createOrReplaceTempView("log")

        spark.sql(
            """
              |select * from log
              |""".stripMargin).show()

        spark.stop()
    }

}
