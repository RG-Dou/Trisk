/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package Nexmark.sources;

import Nexmark.sources.generator.model.BidGeneratorZipf;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.joda.time.DateTime;

import java.util.Objects;
import java.util.Random;

/**
 * A ParallelSourceFunction that generates Nexmark Bid data
 */
public class BidSourceFunction extends RichParallelSourceFunction<Bid> {

    private volatile boolean running = true;
    private GeneratorConfig config;
    private long eventsCountSoFar = 0;
    private int rate;
    private int cycle = 60;
    private int base = 0;
    private int warmUpInterval = 100000;
    private BidGeneratorZipf zipf;
    private String skewField;

    private String skewFieldWarm;

    public BidSourceFunction(int srcRate) {
        this(srcRate, 0);
    }

    public BidSourceFunction(int srcRate, int base) {
        this(srcRate, base, 60);
    }

    public BidSourceFunction(int srcRate, int base, int cycle) {
        this(srcRate, base, cycle, 400000);
    }

    public BidSourceFunction(int srcRate, int base, int cycle, int warmUpInterval) {
        this(srcRate, base, cycle, warmUpInterval, 10000,  1.0);
    }

    public BidSourceFunction(int base, long keys) {
        this(base, keys, 400000);
    }

    public BidSourceFunction(int base, long keys, double skewness){
            this(base, keys, skewness, 400000);
    }

    public BidSourceFunction(int base, long keys, double skewness, int warmUpInterval){
        this(0, base, 60, warmUpInterval, keys, skewness);
    }

    public BidSourceFunction(int srcRate, int base, int cycle, int warmUpInterval, long keys, double skewness){
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
        this.warmUpInterval = warmUpInterval;
        NexmarkConfiguration nexconfig = NexmarkConfiguration.DEFAULT;
        nexconfig.hotAuctionRatio=1;
        config = new GeneratorConfig(nexconfig, 1, 1000L, 0, 1);
        zipf = new BidGeneratorZipf(keys, skewness);
    }


    @Override
    public void run(SourceContext<Bid> ctx) throws Exception {
        long streamStartTime = System.currentTimeMillis();
        int epoch = 0;
        int count = 0;
        int curRate = base + rate;

        // warm up
        Thread.sleep(10000);
        Thread.sleep(warmUpInterval);
//        warmup(ctx);

        long startTs = System.currentTimeMillis();
        long newStartTs = startTs;

        System.out.println("Warm up phase 2");
        curRate = curRate * 3 / 5;
        while (running) {
            if (System.currentTimeMillis() - startTs < warmUpInterval / 4) {
                //Read Warm up
                System.out.println("Bid: epoch: " + epoch % cycle + " current rate is: " + curRate);
                sendEvents(ctx, curRate, skewFieldWarm);
            } else { // after warm up
                if (count == 20) {
                    // change input rate every 1 second.
                    epoch++;
                    curRate = base + Util.changeRateSin(rate, cycle, epoch);
                    System.out.println("Bid: epoch: " + epoch % cycle + " current rate is: " + curRate);
                    count = 0;
                }

//                if(System.currentTimeMillis() - newStartTs >= 60*1000){
//                    newStartTs = System.currentTimeMillis();
//                    base = base + 200;
//                }

                sendEvents(ctx, curRate, skewField);
                count++;
            }
        }
    }

    private void warmup(SourceContext<Bid> ctx) throws InterruptedException {
        int curRate = rate + base/10; //  (sin0 + 1)
        long startTs = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTs < warmUpInterval) {
            sendEvents(ctx, curRate, skewFieldWarm);
        }
    }

    private void sendEvents(SourceContext<Bid> ctx, int curRate, String field) throws InterruptedException {
        long emitStartTime = System.currentTimeMillis();
        for (int i = 0; i < curRate / 20; i++) {

            long nextId = nextId();
            Random rnd = new Random(nextId);

            // When, in event time, we should generate the event. Monotonic.
//            long eventTimestamp =
//                    config.timestampAndInterEventDelayUsForEvent(
//                            config.nextEventNumber(eventsCountSoFar)).getKey();
            Bid bid;
            if (Objects.equals(field, "Auction")){
                bid = zipf.nextBidAucSkew(nextId, rnd, DateTime.now().getMillis(), config);
            } else if (Objects.equals(field, "Random")){
                bid = zipf.nextBidRandom(nextId, rnd, DateTime.now().getMillis(), config);
            } else {
                bid = zipf.nextBid(nextId, rnd, DateTime.now().getMillis(), config);
            }

            ctx.collect(bid);
            eventsCountSoFar++;
        }
        // Sleep for the rest of timeslice if needed
        Util.pause(emitStartTime);
    }

    @Override
    public void cancel() {
        running = false;
    }

    private long nextId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
//        return config.firstEventId + eventsCountSoFar;
    }

    public void setSkewField(String skewField) {
        this.skewField = skewField;
    }

    public void setSkewFieldWarm(String skewFieldWarm) {
        this.skewFieldWarm = skewFieldWarm;
    }
}
