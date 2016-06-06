package examples.bxldirect;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by nvgeele on 04/04/16.
 */
public class RedisReceiver extends Receiver<UserPosition> {
    private Thread runner;

    public RedisReceiver() {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    @Override
    public void onStart() {
        runner = new Thread() {
            @Override
            public void run() {
                JedisPool pool = new JedisPool(new JedisPoolConfig(), RedisConfig.getRedisHost());
                Jedis jedis = null;

                while(true) {
                    try {
                        jedis = pool.getResource();
                        String str = jedis.rpop(BxlDirect.QUEUE);
                        if(str != null)
                            store(UserPosition.fromString(str));
                        Thread.sleep(10);
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        if (jedis != null) {
                            jedis.close();
                        }
                    }
                }
            }
        };
        runner.start();
    }

    @Override
    public void onStop() {
        runner.stop();
    }
}