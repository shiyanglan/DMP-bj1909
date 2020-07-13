/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TagsPlatform
 * Author: yanglan88
 * Date: 2020/6/25 23:31
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/25
 * @since 1.0.0
 */
package com.qf.dmp.context

import com.qf.dmp.traits.TagsTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsPlatform extends TagsTrait{
    /**
     * 打标签方法
     */
    override def makeTags(args: Any*): List[(String, Int)] = {

        //1. 获取结果序列
        var list = List[(String, Int)]()

        //2. 解析参数
        val row: Row = args(0).asInstanceOf[Row]

        //3. 获取到渠道id
        val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")

        //4. 贴标签
        if(StringUtils.isNoneBlank(adplatformproviderid.toString)) {
            list:+=("CN" + adplatformproviderid, 1)
        }
        list
    }
}
