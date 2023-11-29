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

import Nexmark.sources.controllers.AuctionSrcController;
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
    private final AuctionSrcController controller;

    public AuctionSourceFunction(int srcRate) {
        this(srcRate, 0);
    }

    public AuctionSourceFunction(int srcRate, int base) {
        this(srcRate, base, 60);
    }

    public AuctionSourceFunction(int srcBase, long stateSize, AuctionSrcController auctionSrcController){
        this(0, srcBase, 60, 5000, stateSize, auctionSrcController);
    }

    public AuctionSourceFunction(int srcBase, long stateSize, int warmUpInterval, AuctionSrcController auctionSrcController){
        this(0, srcBase, 60, warmUpInterval, stateSize, auctionSrcController);
    }

    public AuctionSourceFunction(int base, long stateSize, int warmUpInterval){
        this(0, base, 60, warmUpInterval, stateSize, null);
    }

    public AuctionSourceFunction(int srcRate, int base, int cycle) {
        this(srcRate, base, cycle, 40000);
    }

    public AuctionSourceFunction(int srcRate, int base, int cycle, int warmUpInterval) {
        this(srcRate, base, cycle, warmUpInterval, 1000, null);
    }

    public AuctionSourceFunction(int srcRate, int base, int cycle, int warmUpInterval, long stateSize, AuctionSrcController auctionSrcController) {
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
        this.warmUpInterval = warmUpInterval;
        NexmarkConfiguration nexConfig = NexmarkConfiguration.DEFAULT;
        nexConfig.avgAuctionByteSize = (int) stateSize;
        config = new GeneratorConfig(nexConfig, 1, 1000L, 0, 1);
        if (auctionSrcController == null)
            this.controller = new AuctionSrcController();
        else
            this.controller = auctionSrcController;
        this.controller.srcFunction = this;
    }

    @Override
    public void run(SourceContext<Auction> ctx) throws Exception {

        controller.beforeRun();

        int epoch = 0;
        int count = 0;
        int curRate = rate;

        // warm up
        Thread.sleep(warmUpInterval);
        while (running) {

            if (count == 20) {
                // change input rate every 1 second.
                epoch++;
                curRate = base + Util.changeRateSin(rate, cycle, epoch);
                System.out.println("Auction: epoch: " + epoch%cycle + " current rate is: " + curRate);
                count = 0;
            }

            sendEvents(ctx, curRate);
            count++;

            controller.checkAndAdjust();
        }
    }

    private void sendEvents(SourceContext<Auction> ctx, int curRate) throws InterruptedException {

      long emitStartTime = System.currentTimeMillis();
        for (int i = 0; i < curRate / 20; i++) {
//            long emitStartTime = System.currentTimeMillis();
            long nextId = nextId();
            Random rnd = new Random(nextId);

            // When, in event time, we should generate the event. Monotonic.
            long eventTimestamp =
                    config.timestampAndInterEventDelayUsForEvent(
                            config.nextEventNumber(eventsCountSoFar)).getKey();

//                ctx.collect(AuctionGenerator.nextAuction(eventsCountSoFar, nextId, rnd, eventTimestamp, config));
            Auction auction = controller.nextAuction(eventsCountSoFar, nextId, rnd, eventTimestamp, config);

            ctx.collect(auction);
            eventsCountSoFar++;
            // Sleep for the rest of timeslice if needed
//            Util.pause(emitStartTime, curRate);
        }
        // Sleep for the rest of timeslice if needed
      Util.pause(emitStartTime);
    }

    @Override
    public void cancel() {
        running = false;
    }

    private long nextId() {
//        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
        return (config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar)) * densityId;
    }

    public void setBase(int base){
        this.base = base;
    }
}