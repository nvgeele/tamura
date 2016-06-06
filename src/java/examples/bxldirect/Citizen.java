package examples.bxldirect;

import java.time.Instant;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Citizen extends Thread {
    private JedisPool pool;
    private Point position;
    private String userId;

    public Citizen() {
        this.pool = new JedisPool(new JedisPoolConfig(), RedisConfig.getRedisHost());
        this.position = new Point(Math.random(), Math.random());
        this.userId = UUID.randomUUID().toString();
    }

    @Override
    public void run() {
        Jedis jedis = null;

        try {
            jedis = pool.getResource();

            while(true) {
                position = new Point(Math.random(), Math.random());
                String toSend = userId + ";" + position.toString() + ";" + Instant.now();
                jedis.lpush(BxlDirect.QUEUE, toSend);

                Thread.sleep(Math.round(Math.random() * 100) + 1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
