package linearRoad.source;

public class DailyExpenditureHistData extends DERequestOrHistData{

    private final int toll;

    public DailyExpenditureHistData(int vid, int xway, int qid, int day, int toll) {
        super(vid, xway, qid, day, false);
        this.toll = toll;
    }

    public int getToll() {
        return toll;
    }
}
