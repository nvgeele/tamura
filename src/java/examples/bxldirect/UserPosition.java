package examples.bxldirect;

import com.google.common.base.Joiner;

import java.io.Serializable;
import java.time.Instant;

/**
 * Created by nvgeele on 04/04/16.
 */
public class UserPosition implements Serializable {
    public static UserPosition fromString(String str) {
        String[] split = str.split(";");
        Point p = Point.fromString(split[1] + ";" + split[2]);
        return new UserPosition(split[0], p, Instant.now());
    }

    public final String userId;
    public final Point position;
    public final Instant date;

    public UserPosition(String userId, Point position, Instant date) {
        this.userId = userId;
        this.position = position;
        this.date = date;
    }

    @Override
    public String toString() {
        return userId + ";" + position.toString();
    }
}
