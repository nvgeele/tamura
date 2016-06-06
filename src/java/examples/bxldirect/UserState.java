package examples.bxldirect;

import java.io.Serializable;
import java.time.Instant;

/**
 * Created by nvgeele on 06/04/16.
 */
public class UserState implements Serializable {
    public final Point lastPosition;
    public final Point.Direction currentDirection;
    public final Point.Direction lastDirection;
    public final Instant date;

    public UserState(Point position, Point.Direction currentDirection, Point.Direction lastDirection) {
        this.lastPosition = position;
        this.currentDirection = currentDirection;
        this.lastDirection = lastDirection;
        this.date = Instant.now();
    }

    public UserState(Point position, Point.Direction currentDirection, Point.Direction lastDirection, Instant date) {
        this.lastPosition = position;
        this.currentDirection = currentDirection;
        this.lastDirection = lastDirection;
        this.date = date;
    }

    @Override
    public String toString() {
        return "<" + this.lastDirection + ", " + this.date + ">";
    }
}