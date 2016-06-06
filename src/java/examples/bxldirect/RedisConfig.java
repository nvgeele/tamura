package examples.bxldirect;

/**
 * Created by nvgeele on 09/04/16.
 */
public class RedisConfig {
    private static String REDIS_HOST = "localhost";

    public static void setRedisHost(String host) {
        REDIS_HOST = host;
    }

    public static String getRedisHost() {
        return REDIS_HOST;
    }
}