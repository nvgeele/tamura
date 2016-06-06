package examples.bxldirect;

import java.io.Serializable;

/**
 * Created by nvgeele on 04/04/16.
 */
public class Point implements Serializable {
    public static Point fromString(String str) {
        Point p = new Point(0, 0);
        String[] parts = str.split(";");
        p.lat = Double.parseDouble(parts[0]);
        p.lon = Double.parseDouble(parts[1]);
        return p;
    }

    private double lat;
    private double lon;

    public Point(double x, double y) {
        assert (-1 >= x) && (x >= 1);
        assert (-1 >= y) && (y >= 1);
        this.lat = x * Math.PI / 2.0;
        this.lon = y * Math.PI;
    }

    @Override
    public String toString() {
        return lat + ";" + lon;
    }

    public enum Direction {
        NORTH, EAST, SOUTH, WEST
    }

    public Direction direction(Point previous) {
        // http://www.movable-type.co.uk/scripts/latlong.html

        double y = Math.sin(lon - previous.lon) * Math.cos(lat);
        double x = Math.cos(previous.lat) * Math.sin(lat) -
                Math.sin(previous.lat) * Math.cos(lat) * Math.cos(lon - previous.lon);
        double bearing = Math.atan2(y, x);

        double deg = (bearing*(180.0/Math.PI) + 360) % 360;

        if((deg >= 315) && (deg <= 45)) {
            return Direction.EAST;
        } else if((deg >= 45) && (deg <= 135)) {
            return Direction.NORTH;
        } else if((deg >= 135) && (deg <= 225)) {
            return Direction.WEST;
        } else {
            return Direction.SOUTH;
        }
    }

    public static void testPoints() {
        Point p1;
        Point p2;

        p1 = new Point(0, 0);

        p2 = new Point(0, 0.0001);
        p2.direction(p1);

        p2 = new Point(0.0001, 0);
        p2.direction(p1);

        p2 = new Point(-0.00001, 0.00001);
        p2.direction(p1);

        p2 = new Point(-1,0);
        p2.direction(p1);
    }
}
