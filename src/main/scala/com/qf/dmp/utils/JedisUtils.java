package com.qf.dmp.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtils {
    private static volatile JedisPool jedisPool;

    private JedisUtils(){}

    /**
     * 获取到连接池对象
     */
    public static JedisPool getJedisPool() {
        if (jedisPool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxWaitMillis(1000 * 300);
            config.setMaxIdle(36);

            jedisPool = new JedisPool(config, "hadoop001", 6379);
        }
        return jedisPool;
    }

    /*
     * 将jedis连接对象返回给连接池
     */
    public static void release(Jedis jedis) {
        if (jedis != null) jedis.close();
    }
}
