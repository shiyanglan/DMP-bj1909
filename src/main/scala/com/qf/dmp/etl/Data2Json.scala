/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Data2Json
 * Author: yanglan88
 * Date: 2020/6/23 11:14
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

import com.qf.dmp.utils.{CommonUtils, Spark_utils}
import org.apache.spark.sql.{DataFrame, SaveMode}

object Data2Json {
    val SPARK_PROPERTIES : String = "spark.properties"

    def main(args: Array[String]): Unit = {

        if (args == null || args.length != 2) {
            println("Usage : <input> <output>")
            System.exit(-1)
        }
        val Array(input, output) = args

        val properties = new Properties()
        properties.load(Data2Json.getClass.getClassLoader.getResourceAsStream(SPARK_PROPERTIES))

        val spark = Spark_utils.getLocalSparkSession(Data2Json.getClass.getName)
        spark.sqlContext.setConf(properties)

        val df: DataFrame = spark.read.parquet(input)

        df.createOrReplaceTempView("log")

        val ret = spark.sql(
            """
              |select
              |count(*) cnt,
              |rank() over(order by count(*) desc) num,
              |provincename,
              |cityname
              |from
              |`log`
              |group by provincename , cityname
              |""".stripMargin).coalesce(1)

        //6.输出
        ret.write.json(output)

        //7. 释放
        Spark_utils.stop(spark)

//        val jdbc = CommonUtils.toMap("db.properties")
//        ret.write.mode(SaveMode.Append).jdbc(jdbc.)
//
//        ret.write.json("")
//
//        Spark_utils.stop(spark)
//6.输出
//        val jdbc = CommonUtils.toMap("db.properties")
//        properties.setProperty("user", jdbc("mysql.username"))
//        properties.setProperty("password", jdbc("mysql.password"))
//        ret.write.mode(SaveMode.Append).jdbc(jdbc("mysql.url"), "dmp", properties)
//        //7. 释放
//        Spark_utils.stop(spark)
    }

}
