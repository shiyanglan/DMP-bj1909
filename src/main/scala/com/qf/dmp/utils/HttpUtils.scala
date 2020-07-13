package com.qf.dmp.utils

import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
 * 发送http请求的类
 */
object HttpUtils {

    /**
     * get请求
     */
    def get(url:String) = {
        //1. 获取到httpclient
        val client = HttpClients.createDefault()

        //2. 创建get请求对象
        val httpGet = new HttpGet(url)

        //3. 发送
        var response: CloseableHttpResponse = client.execute(httpGet)
        val entity: HttpEntity = response.getEntity

        //4. 做编码集限定
        EntityUtils.toString(entity, "UTF-8")
    }
}
