package org.datastreamreductor;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ext_lmarzo on 9/7/17.
 */
public class RedisVisualizer {

    public static void main(String[] args) {
        new RedisVisualizer().run(args[0]);
    }

    public void run(String collection){
        JedisPool jedisPool = new JedisPool("192.168.99.100", 6379);
        Jedis jedis = jedisPool.getResource();

        if (collection.compareToIgnoreCase("hashes") == 0) {
            Object[] array = jedis.keys("hashes*").toArray();
            for (int i = 0; i < array.length; i++) {
                System.out.println("key: " + array[i].toString() + " \n" +
                        "value:" + jedis.get(array[i].toString()));

            }
        }

        if (collection.compareToIgnoreCase("log")==0) {

            SimpleDateFormat sdf = new SimpleDateFormat("yyy-MM-dd hh:mm:ss,SSS");

            String[] array = jedis.keys("log*").toArray(new String[0]);
            List<String> list = Arrays.asList((String[]) array);
            Date[] dates = list.stream().map(s -> {
                try {
                    s = s.replace("log:[", "");
                    return sdf.parse(s);
                } catch (ParseException e) {
                    return null;
                }
            }).toArray(Date[]::new);

            ArrayList<Date> datesList = new ArrayList<>(Arrays.asList(dates));

            datesList.removeAll(Collections.singleton(null));
            Collections.sort(datesList);


            for (Date d: datesList) {
                String strDate = sdf.format(d);
                System.out.println("[" + strDate + "] " + jedis.get("log:[" + strDate));

            }
        }


        jedis.close();
        jedisPool.close();
    }
}
