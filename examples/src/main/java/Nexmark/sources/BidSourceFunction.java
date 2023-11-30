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

import Nexmark.sources.controllers.BidSrcController;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.joda.time.DateTime;

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

    private Boolean inputRateSpy = false;

    private final BidSrcController controller;

    public BidSourceFunction(int srcRate) {
        this(srcRate, 0);
    }

    public BidSourceFunction(int srcRate, int base) {
        this(srcRate, base, 60);
    }

    public BidSourceFunction(int srcRate, int base, int cycle) {
        this(srcRate, base, cycle, 400000, new BidSrcController());
    }

    public BidSourceFunction(int base, int warmUpInterval, BidSrcController controller){
        this(0, base, 60, warmUpInterval, controller);
    }

    public BidSourceFunction(int srcRate, int base, int cycle, int warmUpInterval, BidSrcController controller){
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
        this.warmUpInterval = warmUpInterval;
        NexmarkConfiguration nexconfig = NexmarkConfiguration.DEFAULT;
        nexconfig.hotAuctionRatio=1;
        config = new GeneratorConfig(nexconfig, 1, 1000L, 0, 1);
        this.controller = controller;
    }


    @Override
    public void run(SourceContext<Bid> ctx) throws Exception {
        int epoch = 0;
        int count = 0;
        int curRate = base + rate;

        // warm up
        Thread.sleep(warmUpInterval);

        controller.beforeRun();
        while (running) {

            if (count == 20) {
                // change input rate every 1 second.
                epoch++;
                curRate = base + Util.changeRateSin(rate, cycle, epoch);
                System.out.println("Bid: epoch: " + epoch % cycle + " current rate is: " + curRate);
                count = 0;
            }

            sendEvents(ctx, curRate);
            count++;

            controller.checkAndAdjust();
        }
    }

    private void sendEvents(SourceContext<Bid> ctx, int curRate) throws InterruptedException {
//        long emitStartTime = System.currentTimeMillis();
        for (int i = 0; i < curRate / 20; i++) {
            long emitStartTime = System.currentTimeMillis();

            long nextId = nextId();
            Random rnd = new Random(nextId);

            // When, in event time, we should generate the event. Monotonic.
//            long eventTimestamp =
//                    config.timestampAndInterEventDelayUsForEvent(
//                            config.nextEventNumber(eventsCountSoFar)).getKey();
            Bid bid = controller.nextBid(nextId, rnd, DateTime.now().getMillis(), config);
            ctx.collect(bid);

            eventsCountSoFar++;
            Util.pause(emitStartTime, curRate);
        }
        // Sleep for the rest of timeslice if needed
    }

    @Override
    public void cancel() {
        running = false;
    }

    private long nextId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
//        return config.firstEventId + eventsCountSoFar;
    }

    public int getBase() {
        return base;
    }

    public void setBase(int base) {
        this.base = base;
    }
}
