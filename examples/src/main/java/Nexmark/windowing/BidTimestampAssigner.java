package Nexmark.windowing;

import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public final class BidTimestampAssigner implements AssignerWithPeriodicWatermarks<Bid> {
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
    public long extractTimestamp(Bid element, long previousElementTimestamp) {
        long timestamp = System.currentTimeMillis();
        maxTimestamp = Math.max(maxTimestamp, timestamp);
        return timestamp;
    }
}
