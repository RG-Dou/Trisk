package Nexmark.windowing;

import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public final class PersonTimestampAssigner implements AssignerWithPeriodicWatermarks<Person> {
    private long maxTimestamp = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long now = System.currentTimeMillis();
        long isPunch = now % 1000;
        if((isPunch < 3) || (isPunch > 997))
            return new Watermark(now);
        return null;
//        return new Watermark(maxTimestamp);
    }

    @Override
    public long extractTimestamp(Person element, long previousElementTimestamp) {
        long timestamp = System.currentTimeMillis();
//        maxTimestamp = Math.max(maxTimestamp, element.dateTime);
        maxTimestamp = Math.max(maxTimestamp, timestamp);
        return timestamp;
    }
}
