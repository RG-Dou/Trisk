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
package Nexmark.sources.generator.model;


import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Random;

import static org.apache.beam.sdk.nexmark.sources.generator.model.LongGenerator.nextLong;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.lastBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.nextBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PriceGenerator.nextPrice;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextExtra;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextString;

/** AuctionGenerator. */
public class AuctionGeneratorZipf implements Serializable {
    /**
     * Keep the number of categories small so the example queries will find results even with a small
     * batch of events.
     */
    private static final int NUM_CATEGORIES = 1000;

    /** Number of yet-to-be-created people and auction ids allowed. */
    private static final int AUCTION_ID_LEAD = 10;

    /**
     * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
     * values.
     */
    private static final int HOT_SELLER_RATIO = 100;

    private ZipfUtil zipf;

    private final long keys;
    public long nextID = 0;
//    private static final RandomDataGenerator dataGen = new RandomDataGenerator();

    private int index;
    private int parallel;

    public AuctionGeneratorZipf(long size, double skew){
        this(size, skew, 10000);
    }

    public AuctionGeneratorZipf(long size, double skew, long keys){
        zipf = new ZipfUtil(size, skew);
        this.keys = keys;
    }

    /** Generate and return a random auction with next available id. */
    public Auction nextAuction(
            long eventsCountSoFar, long eventId, Random random, long timestamp, GeneratorConfig config) {

        long id = lastBase0AuctionId(eventId) + GeneratorConfig.FIRST_AUCTION_ID;
        id = nextLong(random, keys);
//        if(random.nextInt(2) == 0)
//            id = 0;

        long seller;
        // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
        if (random.nextInt(config.getHotSellersRatio()) > 0) {
            // Choose the first person in the batch of last HOT_SELLER_RATIO people.
            seller = (lastBase0PersonId(eventId) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
        } else {
            seller = nextBase0PersonId(eventId, random, config);
        }
        seller += GeneratorConfig.FIRST_PERSON_ID;

//        seller = zipf.next();
//        seller = random.nextInt((int) keys);

//        seller = zipf.nextFromFile();

        long category = GeneratorConfig.FIRST_CATEGORY_ID + random.nextInt(NUM_CATEGORIES);
        long initialBid = nextPrice(random);
//        long expires = timestamp + nextAuctionLengthMs(eventsCountSoFar, random, timestamp, config);
        long expires = timestamp + 5000000;
        String name = nextString(random, 20);
        String desc = nextString(random, 100);
        long reserve = initialBid + nextPrice(random);
        int currentSize = 8 + name.length() + desc.length() + 8 + 8 + 8 + 8 + 8;
        String extra = nextExtra(random, currentSize, config.getAvgAuctionByteSize());
        return new Auction(
                id,
                name,
                desc,
                initialBid,
                reserve,
                new Instant(timestamp).getMillis(),
                new Instant(expires).getMillis(),
                seller,
                category,
                extra);
    }

    /** Generate and return a random auction with next available id. */
    public Auction nextAuctionWarmup(
            long eventsCountSoFar, long eventId, Random random, long timestamp, GeneratorConfig config) {

        long id = nextID * parallel + index;
        if(id >= keys)
            return null;
        nextID ++;

        long seller;
        // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
        if (random.nextInt(config.getHotSellersRatio()) > 0) {
            // Choose the first person in the batch of last HOT_SELLER_RATIO people.
            seller = (lastBase0PersonId(eventId) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
        } else {
            seller = nextBase0PersonId(eventId, random, config);
        }
        seller += GeneratorConfig.FIRST_PERSON_ID;

        long category = GeneratorConfig.FIRST_CATEGORY_ID + random.nextInt(NUM_CATEGORIES);
//        long initialBid = nextPrice(random);
        long initialBid = 100_000_000;
//        long expires = timestamp + nextAuctionLengthMs(eventsCountSoFar, random, timestamp, config);
        long expires = timestamp + 5000000;
        String name = nextString(random, 20);
        String desc = nextString(random, 100);
        long reserve = initialBid + nextPrice(random);
        int currentSize = 8 + name.length() + desc.length() + 8 + 8 + 8 + 8 + 8;
        String extra = nextExtra(random, currentSize, config.getAvgAuctionByteSize());
        return new Auction(
                id,
                name,
                desc,
                initialBid,
                reserve,
                new Instant(timestamp).getMillis(),
                new Instant(expires).getMillis(),
                seller,
                category,
                extra);
    }

    /** Generate and return a random auction with next available id. */
    public Auction nextAuctionSellSkew(
            long eventsCountSoFar, long eventId, Random random, long timestamp, GeneratorConfig config) {

        long id = lastBase0AuctionId(eventId) + GeneratorConfig.FIRST_AUCTION_ID;

        long seller = random.nextInt((int) keys);


        long category = GeneratorConfig.FIRST_CATEGORY_ID + random.nextInt(NUM_CATEGORIES);
        long initialBid = nextPrice(random);
//        long expires = timestamp + nextAuctionLengthMs(eventsCountSoFar, random, timestamp, config);
        long expires = timestamp + 5000000;
        String name = nextString(random, 20);
        String desc = nextString(random, 100);
        long reserve = initialBid + nextPrice(random);
        int currentSize = 8 + name.length() + desc.length() + 8 + 8 + 8 + 8 + 8;
        String extra = nextExtra(random, currentSize, config.getAvgAuctionByteSize());
        return new Auction(
                id,
                name,
                desc,
                initialBid,
                reserve,
                new Instant(timestamp).getMillis(),
                new Instant(expires).getMillis(),
                seller,
                category,
                extra);
    }

    /**
     * Return the last valid auction id (ignoring FIRST_AUCTION_ID). Will be the current auction id if
     * due to generate an auction.
     */
    public static long lastBase0AuctionId(long eventId) {
        long epoch = eventId / GeneratorConfig.PROPORTION_DENOMINATOR;
        long offset = eventId % GeneratorConfig.PROPORTION_DENOMINATOR;
        if (offset < GeneratorConfig.PERSON_PROPORTION) {
            // About to generate a person.
            // Go back to the last auction in the last epoch.
            epoch--;
            offset = GeneratorConfig.AUCTION_PROPORTION - 1;
        } else if (offset >= GeneratorConfig.PERSON_PROPORTION + GeneratorConfig.AUCTION_PROPORTION) {
            // About to generate a bid.
            // Go back to the last auction generated in this epoch.
            offset = GeneratorConfig.AUCTION_PROPORTION - 1;
        } else {
            // About to generate an auction.
            offset -= GeneratorConfig.PERSON_PROPORTION;
        }
        return epoch * GeneratorConfig.AUCTION_PROPORTION + offset;
    }

    /** Return a random auction id (base 0). */
    public static long nextBase0AuctionId(long nextEventId, Random random, GeneratorConfig config) {

        // Choose a random auction for any of those which are likely to still be in flight,
        // plus a few 'leads'.
        // Note that ideally we'd track non-expired auctions exactly, but that state
        // is difficult to split.
        long minAuction =
                Math.max(lastBase0AuctionId(nextEventId) - config.getNumInFlightAuctions(), 0);
        long maxAuction = lastBase0AuctionId(nextEventId);
        return minAuction + nextLong(random, maxAuction - minAuction + 1 + AUCTION_ID_LEAD);
    }

    /** Return a random time delay, in milliseconds, for length of auctions. */
    private static long nextAuctionLengthMs(
            long eventsCountSoFar, Random random, long timestamp, GeneratorConfig config) {

        // What's our current event number?
        long currentEventNumber = config.nextAdjustedEventNumber(eventsCountSoFar);
        // How many events till we've generated numInFlightAuctions?
        long numEventsForAuctions =
                ((long) config.getNumInFlightAuctions() * GeneratorConfig.PROPORTION_DENOMINATOR)
                        / GeneratorConfig.AUCTION_PROPORTION;
        // When will the auction numInFlightAuctions beyond now be generated?
        long futureAuction =
                config
                        .timestampAndInterEventDelayUsForEvent(currentEventNumber + numEventsForAuctions)
                        .getKey();
        // System.out.printf("*** auction will be for %dms (%d events ahead) ***\n",
        //     futureAuction - timestamp, numEventsForAuctions);
        // Choose a length with average horizonMs.
        long horizonMs = futureAuction - timestamp;
        return 1L + nextLong(random, Math.max(horizonMs * 2, 1L));
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }
}