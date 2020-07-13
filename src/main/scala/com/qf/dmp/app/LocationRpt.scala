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

import com.qf.dmp.etl.Data2Json.SPARK_PROPERTIES
import com.qf.dmp.etl.Log2Parquet
import com.qf.dmp.traits.Logger_Trait
import com.qf.dmp.utils.Spark_utils
import org.apache.spark.sql.{DataFrame, SparkSession}

object LocationRpt extends Logger_Trait{

    val SPARK_PROPERTIES : String = "spark.properties"

    def main(args: Array[String]): Unit = {

        if (args == null || args.length != 2) {
            println("Usage : <input> <output>")
            System.exit(-1)
        }
        val Array(input, output) = args

        val properties = new Properties()
        properties.load(Log2Parquet.getClass.getClassLoader.getResourceAsStream(SPARK_PROPERTIES))

        val spark: SparkSession = Spark_utils.getLocalSparkSession(LocationRpt.getClass.getName)
        spark.sqlContext.setConf(properties)

        val df: DataFrame = spark.read.parquet(input)

        df.createOrReplaceTempView("log")

        val ret: DataFrame = spark.sql(
            """
              |select provincename, cityname,
              |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) ys_request_cnt,
              |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) yx_request_cnt,
              |sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) ad_request_cnt,
              |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cy_bid_cnt,
              |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) bid_succ_cnt,
              |sum(case when iseffective = 1 and requestmode = 2 then 1 else 0 end) show_cnt,
              |sum(case when iseffective = 1 and requestmode = 3 then 1 else 0 end) click_cnt,
              |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice / 1000 else 0.0 end) price_cnt,
              |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment / 1000 else 0.0 end) ad_cnt
              |from log
              |group by provincename, cityname
              |""".stripMargin).coalesce(1)

        ret.show()

        ret.write.partitionBy("provincename","cityname").save(output)

        Spark_utils.stop(spark)

    }
}
