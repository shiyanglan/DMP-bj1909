/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TagsTrait
 * Author: yanglan88
 * Date: 2020/6/24 11:25
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/24
 * @since 1.0.0
 */
package com.qf.dmp.traits

trait TagsTrait {

    /*
    打标签方法
     */
    def makeTags(args:Any*):List[(String,Int)]
}
