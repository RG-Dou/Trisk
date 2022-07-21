package linearRoad.source;

import java.io.Serializable;

public class DERequestOrHistData implements Serializable {
    final int vid;
    private final int xway;
    private final int qid;
    private final int day;
    private final boolean request;

    public DERequestOrHistData(int vid, int xway, int qid, int day, boolean request) {
        this.vid = vid;
        this.xway = xway;
        this.qid = qid;
        this.day = day;
        this.request = request;
    }

    public int getDay() {
        return day;
    }

    public int getQid() {
        return qid;
    }

    public int getVid() {
        return vid;
    }

    public int getXway() {
        return xway;
    }

    public boolean isRequest() {
        return request;
    }

    public String toString() {
        return " vid:" + vid + " xway:" + xway + " qid:" + qid + " day:" + day;
    }
}
