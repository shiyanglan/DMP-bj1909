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

object Data2Mysql {
    val SPARK_PROPERTIES : String = "spark.properties"

    def main(args: Array[String]): Unit = {

        if (args == null || args.length != 2) {
            println("Usage : <input> <output>")
            System.exit(-1)
        }
        val Array(input, output) = args

        val properties = new Properties()
        properties.load(Log2Parquet.getClass.getClassLoader.getResourceAsStream(SPARK_PROPERTIES))

        val spark = Spark_utils.getLocalSparkSession(Data2Mysql.getClass.getName)
        spark.sqlContext.setConf(properties)

        val df: DataFrame = spark.read.parquet(input)

        df.createOrReplaceTempView("log")

        val ret: DataFrame = spark.sql(
            """
              |select
              |count(*) ct,
              |provincename,
              |cityname
              |from
              |`log`
              |group by provincename , cityname
              |""".stripMargin).coalesce(1)


        val jdbc = CommonUtils.toMap("db.properties")

        properties.setProperty("user", jdbc("mysql.username"))
        properties.setProperty("password", jdbc("mysql.password"))

        ret.write.mode(SaveMode.Append).jdbc(jdbc("mysql.url"),"dmp",properties)

        Spark_utils.stop(spark)
    }

}
