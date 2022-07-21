package linearRoad;

import linearRoad.source.DERequestOrHistData;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class RequestTimestampAssigner implements AssignerWithPeriodicWatermarks<DERequestOrHistData> {
    private long maxTimestamp = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long now = System.currentTimeMillis();
        long isPunch = now % 1000;
        if((isPunch < 2) || (isPunch > 998))
            return new Watermark(now);
        return null;
    }

    @Override
    public long extractTimestamp(DERequestOrHistData element, long previousElementTimestamp) {
        long timestamp = System.currentTimeMillis();
        maxTimestamp = Math.max(maxTimestamp, timestamp);
        return timestamp;
    }
}
