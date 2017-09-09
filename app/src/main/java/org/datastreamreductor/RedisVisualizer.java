package org.datastreamreductor;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Created by ext_lmarzo on 9/7/17.
 */
public class RedisVisualizer {

    public static void main(String[] args) {
        new RedisVisualizer().run();
    }

    public void run(){
        JedisPool jedisPool = new JedisPool("192.168.99.100", 6379);
        Jedis jedis = jedisPool.getResource();
       // jedis.set("log:estoeslog", "stacktrace");
        //System.out.println(jedis.keys("log*").size());

        Object[] array = jedis.keys("*").toArray();
        for (int i = 0; i < array.length; i++) {
            System.out.println("key: " + array[i].toString() + " \n " +
                    "value: " + jedis.get(array[i].toString()));

        }
        jedis.close();
        jedisPool.close();
    }
}
