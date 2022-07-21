package linearRoad.source;

import java.io.Serializable;

public class DailyExpenditureRequest extends DERequestOrHistData {

    public long getTimestamp() {
        return timestamp;
    }

    private final long timestamp;

    public DailyExpenditureRequest(long timestamp, int vid, int xway, int qid, int day) {
        super(vid, xway, qid, day, true);
        this.timestamp = timestamp;
    }
}
