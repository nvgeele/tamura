package examples.bxldirect;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Tuple2;

import java.time.Duration;
import java.time.Instant;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class BxlDirect {
    static final int TIMEOUT = 5;
    static final int NUM_THREADS = 10;
    static final String QUEUE = "bxlqueue";

    public static void main(String[] args) {
        boolean local = args.length > 0 && args[0].equals("1");
        String checkpointDir = "/exports/home/nvgeele/checkpoint";

        if(args.length > 1)
            RedisConfig.setRedisHost(args[1]);

        SparkConf sparkConf = new SparkConf()
                .setAppName("NetCount");

        if(local) {
            sparkConf = sparkConf.setMaster("local[*]");
            checkpointDir = "/tmp/checkpoint";
        }

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc,
                Durations.seconds(1));

        ssc.checkpoint(checkpointDir);

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
                            Duration d = Duration.between(state.date, pos.date);
                            if(d.getSeconds() > TIMEOUT) {
                                s = new UserState(pos.position, null, state.currentDirection, pos.date);
                            } else {
                                s = new UserState(pos.position,
                                        pos.position.direction(state.lastPosition),
                                        state.currentDirection,
                                        pos.date);
                            }
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
                        Duration d = Duration.between(state.date, Instant.now());
                        return state.currentDirection != null && d.getSeconds() <= TIMEOUT;
                    }
                });

        filtered.count().print();

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
        }).print();

        ssc.start();

        if(local) {
            JedisPool pool = new JedisPool(new JedisPoolConfig(), RedisConfig.getRedisHost());
            pool.getResource().del(QUEUE);

            // Spawn threads
            for (int i = 0; i < NUM_THREADS; i++) {
                (new Citizen()).start();
            }
        }

        ssc.awaitTermination();
    }
}