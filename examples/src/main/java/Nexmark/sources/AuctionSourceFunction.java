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

import Nexmark.sources.generator.model.AuctionGeneratorZipf;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.joda.time.DateTime;

import java.util.Objects;
import java.util.Random;

/**
 * A ParallelSourceFunction that generates Nexmark Person data
 */
public class AuctionSourceFunction extends RichParallelSourceFunction<Auction> {

    private volatile boolean running = true;
//    private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
    private GeneratorConfig config;
    private long eventsCountSoFar = 0;
    private int rate;
    private int cycle = 60;
    private int base = 0;
    private int warmUpInterval = 60000;
    private int densityId = 25;
    private AuctionGeneratorZipf generatorZipf;

    private String skewField;
    private boolean warmUp = true;

    public AuctionSourceFunction(int srcRate) {
        this(srcRate, 0);
    }

    public AuctionSourceFunction(int srcRate, int base) {
        this(srcRate, base, 60);
    }

    public AuctionSourceFunction(int srcBase, long stateSize){
        this(0, srcBase, 60);
        NexmarkConfiguration nexConfig = NexmarkConfiguration.DEFAULT;
        nexConfig.avgAuctionByteSize = (int) stateSize;
        config = new GeneratorConfig(nexConfig, 1, 1000L, 0, 1);
//        long size = 1000000000 / stateSize;
//        generatorZipf = new AuctionGeneratorZipf(size, 0.6);
    }

    public AuctionSourceFunction(int base, long stateSize, long keys){
        this(base, stateSize, keys, 1.0);
    }

    public AuctionSourceFunction(int base, long stateSize, long keys, double skewness){
        this(base, stateSize, keys, skewness, 10000);
    }

    public AuctionSourceFunction(int base, long stateSize, long keys, double skewness, int warmUpInterval){
        this(0, base, 60, warmUpInterval, stateSize, skewness, keys);
    }


    public AuctionSourceFunction(int srcRate, int base, int cycle) {
        this(srcRate, base, cycle, 40000);
    }

    public AuctionSourceFunction(int srcRate, int base, int cycle, int warmUpInterval) {
        this(srcRate, base, cycle, warmUpInterval, 1000, 1.0, 100000);
    }

    public AuctionSourceFunction(int srcRate, int base, int cycle, int warmUpInterval, long stateSize, double skewness, long keys) {
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
        this.warmUpInterval = warmUpInterval;
        NexmarkConfiguration nexConfig = NexmarkConfiguration.DEFAULT;
        nexConfig.avgAuctionByteSize = (int) stateSize;
        config = new GeneratorConfig(nexConfig, 1, 1000L, 0, 1);
        generatorZipf = new AuctionGeneratorZipf(stateSize, skewness, keys);
    }

    @Override
    public void run(SourceContext<Auction> ctx) throws Exception {
        generatorZipf.setIndex(getRuntimeContext().getIndexOfThisSubtask());
        generatorZipf.setParallel(getRuntimeContext().getNumberOfParallelSubtasks());

        int epoch = 0;
        int count = 0;
        int curRate = rate;

        // warm up
        Thread.sleep(30000);
        warmup(ctx);
        System.out.println("number of events: " + eventsCountSoFar);

        while (running) {
            long emitStartTime = System.currentTimeMillis();

            if (count == 20) {
                // change input rate every 1 second.
                epoch++;
                curRate = base + Util.changeRateSin(rate, cycle, epoch);
                System.out.println("Auction: epoch: " + epoch%cycle + " current rate is: " + curRate);
                count = 0;
            }

            sendEvents(ctx, curRate, "");

            // Sleep for the rest of timeslice if needed
            Util.pause(emitStartTime);
            count++;
        }
    }

    private void warmup(SourceContext<Auction> ctx) throws InterruptedException {
        int curRate = rate + base; //  (sin0 + 1) * rate + base
        curRate = 500000;
        long startTs = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTs < warmUpInterval) && warmUp) {
            long emitStartTime = System.currentTimeMillis();

            sendEvents(ctx, curRate, skewField);
            // Sleep for the rest of timeslice if needed
            Util.pause(emitStartTime);
        }
    }

    private void sendEvents(SourceContext<Auction> ctx, int curRate, String field){

        for (int i = 0; i < curRate / 20; i++) {
            long nextId = nextId();
            Random rnd = new Random(nextId);

            // When, in event time, we should generate the event. Monotonic.
            long eventTimestamp =
                    config.timestampAndInterEventDelayUsForEvent(
                            config.nextEventNumber(eventsCountSoFar)).getKey();

//                ctx.collect(AuctionGenerator.nextAuction(eventsCountSoFar, nextId, rnd, eventTimestamp, config));
            Auction auction;
            if(Objects.equals(field, "Warmup")){
                auction = generatorZipf.nextAuctionWarmup(eventsCountSoFar, nextId, rnd, DateTime.now().getMillis(), config);
                if (auction == null) {
                    System.out.println("Stop warm up");
                    warmUp = false;
                    skewField = "";
                    return;
                }
            } else if(Objects.equals(field, "Seller")) {
                auction = generatorZipf.nextAuctionSellSkew(eventsCountSoFar, nextId, rnd, DateTime.now().getMillis(), config);
            } else {
                auction=generatorZipf.nextAuction(eventsCountSoFar, nextId, rnd, DateTime.now().getMillis(), config);
            }

            ctx.collect(auction);
            eventsCountSoFar++;
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private long nextId() {
//        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
        return (config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar)) * densityId;
    }

    public void setSkewField(String skewField) {
        this.skewField = skewField;
    }

    public void setCategory(int category){
        generatorZipf.setNUM_CATEGORIES(category);
    }
}