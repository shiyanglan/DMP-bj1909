/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TagsUtils
 * Author: yanglan88
 * Date: 2020/6/24 11:14
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/24
 * @since 1.0.0
 */
package com.qf.dmp.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsUtils {

    val onUserId: String = "imei != '' or mac != '' or idfa != '' or openudid != '' or androidid != '' "

    def getAnyOneUserId(row:Row) = {
        row match {
            case v if StringUtils.isNoneBlank(v.getAs[String]("imei")) => "IM:" + v.getAs[String]("imei")
            case v if StringUtils.isNoneBlank(v.getAs[String]("mac")) => "MC:" + v.getAs[String]("mac")
            case v if StringUtils.isNoneBlank(v.getAs[String]("idfa")) => "IDFA:" + v.getAs[String]("idfa")
            case v if StringUtils.isNoneBlank(v.getAs[String]("openudid")) => "OID:" + v.getAs[String]("openudid")
            case v if StringUtils.isNoneBlank(v.getAs[String]("androidid")) => "AID:" + v.getAs[String]("androidid")
        }
    }

    /**
     * 获取到所有不为空的userid
     */
    def getAnyAllUserId(row:Row):List[String] = {
        var list = List[String]()
        if (StringUtils.isNoneBlank(row.getAs[String]("imei"))) list :+= "IM:" + row.getAs[String]("imei")
        if (StringUtils.isNoneBlank(row.getAs[String]("mac")))  list :+= "MC:" + row.getAs[String]("mac")
        if (StringUtils.isNoneBlank(row.getAs[String]("idfa")))  list :+= "IDFA:" + row.getAs[String]("idfa")
        if (StringUtils.isNoneBlank(row.getAs[String]("openudid")))  list :+= "OID:" + row.getAs[String]("openudid")
        if (StringUtils.isNoneBlank(row.getAs[String]("androidid")))  list :+= "AID:" + row.getAs[String]("androidid")
        list
    }
}
