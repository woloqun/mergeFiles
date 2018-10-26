package com.fan.merge;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Component("redispools")
public class JedisPoolz {

    @Value("${redis.host:localhost}")
    private String redisHost;

    @Value("${redis.port:6379}")
    private int redisPort;

    private JedisPool pool = null;

    private JedisPool getPool() {
        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(500);
            config.setMaxIdle(5);
            config.setMaxWaitMillis(1000 * 10);
            config.setTestOnBorrow(true);
            pool = new JedisPool(config, redisHost, redisPort, 10000);

        }
        return pool;
    }

    public Jedis getResource() {
        if (pool == null) {
            synchronized (JedisPoolz.class) {
                if (pool == null) {
                    pool = getPool();
                }
            }
        }
        return pool.getResource();
    }

}
