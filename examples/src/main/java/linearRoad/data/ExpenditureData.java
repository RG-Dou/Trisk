package linearRoad.data;

import java.io.Serializable;
import java.util.Random;

public class ExpenditureData implements Serializable {

    private final int xway;
    private final int vid;
    private final int day;
    private final int toll;
    private final long timestamp;
    private final String extra;

    public ExpenditureData(int xway, int day, int vid, int toll, int extraSize){
        this.xway = xway;
        this.day = day;
        this.vid = vid;
        this.toll = toll;
        timestamp = System.currentTimeMillis();
        extra = nextExtra(new Random(), extraSize);
    }

    public static String nextExtra(Random random, int desiredAverageSize) {
        int delta = (int)Math.round((double)desiredAverageSize * 0.2D);
        int minSize = desiredAverageSize - delta;
        int desiredSize = minSize + (delta == 0 ? 0 : random.nextInt(2 * delta));
        return nextExactString(random, desiredSize);
    }

    public static String nextExactString(Random random, int length) {
        StringBuilder sb = new StringBuilder();
        while(length-- > 0) {
            sb.append((char)(97 + random.nextInt(26)));
        }
        return sb.toString();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getXway() {
        return xway;
    }

    public int getVid() {
        return vid;
    }

    public int getDay() {
        return day;
    }

    public int getToll() {
        return toll;
    }

    @Override
    public String toString() {
        return "ExpenditureData{" +
                "xway=" + xway +
                ", vid=" + vid +
                ", day=" + day +
                ", toll=" + toll +
                ", timestamp=" + timestamp +
                ", extra='" + extra + '\'' +
                '}';
    }
}
