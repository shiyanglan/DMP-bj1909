/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: LocationRpt
 * Author: yanglan88
 * Date: 2020/6/23 14:38
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/23
 * @since 1.0.0
 */
package com.qf.dmp.app

import java.util.Properties

import cn.qphone.dmp.utils.RtbUtils
import com.qf.dmp.etl.Log2Parquet
import com.qf.dmp.traits.Logger_Trait
import com.qf.dmp.utils.Spark_utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object LocationRptV2 extends Logger_Trait{

    val SPARK_PROPERTIES : String = "spark.properties"

    def main(args: Array[String]): Unit = {

        if (args == null || args.length != 2) {
            println("Usage : <input> <output>")
            System.exit(-1)
        }
        val Array(input, output) = args

        val properties = new Properties()
        properties.load(LocationRptV2.getClass.getClassLoader.getResourceAsStream(SPARK_PROPERTIES))

        val spark: SparkSession = Spark_utils.getLocalSparkSession(LocationRptV2.getClass.getName)
        spark.sqlContext.setConf(properties)

        //3. 获取数据
        val df = spark.read.parquet(input)

        val words: RDD[((String, String), List[Double])] = df.rdd.map(row => {
            //4.1 获取需要的字段
            val requestmode:Int = row.getAs[Int]("requestmode")
            val processnode = row.getAs[Int]("processnode")
            val iseffective = row.getAs[Int]("iseffective")
            val isbilling = row.getAs[Int]("isbilling")
            val isbid = row.getAs[Int]("isbid")
            val iswin = row.getAs[Int]("iswin")
            val adorderid = row.getAs[Int]("iswin")
            val winprice = row.getAs[Double]("winprice")
            val adpayment = row.getAs[Double]("adpayment")

            //4.2 处理业务
            val reqList:List[Double] = RtbUtils.requestAd(requestmode, processnode) // 告诉我你到底是一个什么请求
            val adList:List[Double] = RtbUtils.adPrice(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment) // 竞价和广告
            val clickList: List[Double] = RtbUtils.shows(requestmode, iseffective) // 展示和点击

            //4.3 获取省市
            val pro = row.getAs[String]("provincename")
            val city = row.getAs[String]("cityname")
            ((pro, city), reqList ++ adList ++ clickList)
        })//.foreach(println)

//        5. 聚合
        val value: RDD[((String, String), List[Double])] = words
            .reduceByKey((list1, list2) => {
            list1.zip(list2).map(t => t._1 + t._2)
        })
        value.foreach(println)


        //6. 释放资源
        Spark_utils.stop(spark)

    }

}
