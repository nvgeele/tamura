package examples.bxldirect;

import com.google.common.base.Optional;
import examples.BxlHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Tuple2;

import java.time.Duration;
import java.time.Instant;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class BxlDirect {
    static private int NUM_THREADS = 10;
    static public String QUEUE;
    static public String OUT_QUEUE;

    static private long start_time;

    static private String checkpointDir;
    static private JavaSparkContext sc;
    static private JavaStreamingContext ssc;

    static private int duration = 1000;

    static Jedis outConn;

    public static void setCheckpointDir(String x) {
        checkpointDir = x;
    }

    public static void setSparkContext(JavaSparkContext x) {
        sc = x;
    }

    public static void setRedisHost(String host) {
        RedisConfig.setRedisHost(host);
    }

    public static void setRedisKey(String key) {
        QUEUE = key;
    }

    public static void setRedisOutKey(String key)
    {
        OUT_QUEUE = key;
    }

    public static void setDuration(int d) {
        duration = d;
    }

    public static void spawnMessages(Jedis conn, int users, int updates) {
        Instant date;
        Point point;
        Instant start = Instant.now();
        for(int user_id = 0; user_id < users; user_id++) {
            date = start.plus(Duration.ofSeconds(1));
            for(int n = 0; n < updates; n++) {
                point = new Point(Math.random(), Math.random());
                String toSend = user_id + ";" + point.toString() + ";" + date.toString();
                conn.lpush(BxlDirect.QUEUE, toSend);
            }
        }
        System.out.println("Pushed messages!");
    }

    public static void createPollThread(Jedis conn) {
        final Jedis c = conn;
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    if(c.llen(BxlDirect.QUEUE) == 0) {
                        long finished = System.nanoTime();
                        double time = (double)(finished - start_time)/1000000.0;
                        List a = BxlHelper.getAppended();
                        System.out.println("Appended: " + a.size());
                        System.out.println("Time: " + time);
                        System.out.println("We done here");
                        stop();
                        System.exit(0);
                    } else {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        t.start();
    }

    public static void createGraph() {
        JavaDStream<UserPosition> positions = ssc.receiverStream(new RedisReceiver());

        JavaPairDStream<String, UserPosition> positionPairs =
                positions.mapToPair(new PairFunction<UserPosition, String, UserPosition>() {
                    public Tuple2<String, UserPosition> call(UserPosition userPosition) throws Exception {
                        return new Tuple2<String, UserPosition>(userPosition.userId, userPosition);
                    }
                });

        // <KeyType, Optional<ValueType>, State<StateType>, MapType>
        Function3<String, Optional<UserPosition>, State<UserState>, Tuple2<String, UserState>> mappingFunc =
                new Function3<String, Optional<UserPosition>, State<UserState>, Tuple2<String, UserState>>() {
                    public Tuple2<String, UserState> call(String id, Optional<UserPosition> userPositionOptional, State<UserState> userStateState) throws Exception {
                        UserState s;
                        UserPosition pos = userPositionOptional.get();

                        if(userStateState.exists()) {
                            UserState state = userStateState.get();
                            s = new UserState(pos.position,
                                    pos.position.direction(state.lastPosition),
                                    state.currentDirection,
                                    pos.date);
                        } else {
                            s = new UserState(pos.position, null, null, pos.date);
                        }

                        userStateState.update(s);
                        return new Tuple2<String, UserState>(id, s);
                    }
                };

        JavaMapWithStateDStream<String, UserPosition, UserState, Tuple2<String, UserState>> stateful =
                positionPairs.mapWithState(StateSpec.function(mappingFunc));

        JavaPairDStream<String, UserState> filtered = stateful
                .stateSnapshots().filter(new Function<Tuple2<String, UserState>, Boolean>() {
                    public Boolean call(Tuple2<String, UserState> stringUserStateTuple2) throws Exception {
                        UserState state = stringUserStateTuple2._2();
                        return state.currentDirection != null;
                    }
                });

        filtered.map(new Function<Tuple2<String,UserState>, Point.Direction>() {
            public Point.Direction call(Tuple2<String, UserState> stringUserStateTuple2) throws Exception {
                return stringUserStateTuple2._2().currentDirection;
            }
        }).countByValue().reduce(new Function2<Tuple2<Point.Direction, Long>, Tuple2<Point.Direction, Long>, Tuple2<Point.Direction, Long>>() {
            public Tuple2<Point.Direction, Long> call(Tuple2<Point.Direction, Long> directionLongTuple2, Tuple2<Point.Direction, Long> directionLongTuple22) throws Exception {
                if(directionLongTuple2._2() > directionLongTuple22._2())
                    return directionLongTuple2;
                else
                    return directionLongTuple22;
            }
        }).foreachRDD(new VoidFunction2<JavaRDD<Tuple2<Point.Direction, Long>>, Time>() {
            @Override
            public void call(JavaRDD<Tuple2<Point.Direction, Long>> tuple2JavaRDD, Time time) throws Exception {
//                BxlHelper.append(tuple2JavaRDD);
                outConn.rpush(OUT_QUEUE, tuple2JavaRDD.toString());
            }
        });
    }

    static private void setupStreamingContext() {
        ssc = new JavaStreamingContext(sc, Durations.milliseconds(duration));
        ssc.checkpoint(checkpointDir);
    }

    public static void start() {
        setupStreamingContext();
        createGraph();
        outConn = new Jedis(RedisConfig.getRedisHost());
        ssc.start();
    }

    public static void stop() {
        ssc.stop(false);
        outConn.close();
    }

    public static void main(String[] args) {
        boolean local = args.length > 0 && args[0].equals("1");

        SparkConf sparkConf = new SparkConf()
                .setAppName("NetCount");

        if(local) {
            sparkConf = sparkConf.setMaster("local[*]");
            setCheckpointDir("/tmp/checkpoint");
        } else {
            setCheckpointDir("/exports/home/nvgeele/checkpoint");
        }

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        setSparkContext(sc);

        setRedisHost("localhost");
        setRedisKey("bxlqueue");

        if(local && args.length <= 1) {
            start();
            JedisPool pool = new JedisPool(new JedisPoolConfig(), RedisConfig.getRedisHost());
            pool.getResource().del(QUEUE);

            // Spawn threads
            for (int i = 0; i < NUM_THREADS; i++) {
                (new Citizen()).start();
            }
            ssc.awaitTermination();
        } else if(local && args.length == 3) {
            int users = Integer.parseInt(args[1]);
            int updates = Integer.parseInt(args[2]);
            Jedis conn = new Jedis(RedisConfig.getRedisHost());
            spawnMessages(conn, users, updates);
            start();
            start_time = System.nanoTime();
            createPollThread(conn);
            ssc.awaitTermination();
        }
    }
}