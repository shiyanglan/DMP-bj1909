/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TagsRegion
 * Author: yanglan88
 * Date: 2020/6/26 11:52
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/26
 * @since 1.0.0
 */
package com.qf.dmp.context

import com.qf.dmp.traits.TagsTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsRegion extends TagsTrait{
    override def makeTags(args: Any*): List[(String, Int)] = {

        val list: List[(String, Int)] = List[(String, Int)]()

        val row: Row = args(0).asInstanceOf[Row]

        val rtbprovince: String = row.getAs[String]("provincename")
        val rtbcity: String = row.getAs[String]("cityname")

        var ZPrtbprovince : (String, Int) = null
        var ZCrtbcity : (String, Int) = null

        if (StringUtils.isNoneBlank(rtbprovince) || StringUtils.isNoneBlank(rtbcity)) {
            ZPrtbprovince = ("ZP" + rtbprovince, 1)
            ZCrtbcity = ("ZC" + rtbcity, 1)
        }

        list :+ ZPrtbprovince :+ ZCrtbcity
    }
}
