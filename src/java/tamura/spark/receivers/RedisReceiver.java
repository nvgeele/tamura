package tamura.spark.receivers;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisReceiver extends Receiver<String> {
    private Thread runner;
    private String host;
    private String key;

    public RedisReceiver(String host, String key) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.host = host;
        this.key = key;
    }

    @Override
    public void onStart() {
        runner = new Thread() {
            @Override
            public void run() {
                JedisPool pool = new JedisPool(new JedisPoolConfig(), host);
                Jedis jedis = null;

                while(true) {
                    try {
                        jedis = pool.getResource();
                        String str = jedis.blpop(0, key).get(1);
                        if(str != null)
                            store(str);
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
        runner.interrupt();
        new Jedis(host).lpush(key, "");
    }
}