package examples;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisReceiver extends Receiver<String> {
    private Thread runner;
    private final String _host;
    private final String _key;

    public RedisReceiver(String host, String key) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        _host = host;
        _key = key;
    }

    @Override
    public void onStart() {
        runner = new Thread() {
            @Override
            public void run() {
                JedisPool pool = new JedisPool(new JedisPoolConfig(), _host);
                Jedis jedis = null;

                while(true) {
                    try {
                        jedis = pool.getResource();
                        String str = jedis.blpop(0, _key).get(1);
                        if(str != null)
                            store(str);
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