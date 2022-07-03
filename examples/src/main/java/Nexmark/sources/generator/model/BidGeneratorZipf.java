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

import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.PriceGenerator;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Random;

import static org.apache.beam.sdk.nexmark.sources.generator.model.LongGenerator.nextLong;
import static org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator.lastBase0AuctionId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator.nextBase0AuctionId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.lastBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.nextBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextExtra;

/** Generates bids. */
public class BidGeneratorZipf implements Serializable {

    /**
     * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
     * values.
     */
    private static final int HOT_AUCTION_RATIO = 100;

    private static final int HOT_BIDDER_RATIO = 100;

    private static final RandomDataGenerator dataGen = new RandomDataGenerator();

    private final long keys;
    private final int skewKeys = 10;
//    private final int skewKeys = 1000;
    private final double skewness;
    private ZipfUtil zipf;

    public BidGeneratorZipf(long keys, double skewness){
        this.keys = keys;
        this.skewness = skewness;
        zipf = new ZipfUtil(keys, 1000);
    }

    /** Generate and return a random bid with next available id. */
    public Bid nextBid(long eventId, Random random, long timestamp, GeneratorConfig config) {

        long auction;
        // Here P(bid will be for a hot auction) = 1 - 1/hotAuctionRatio.
        if (random.nextInt(config.getHotAuctionRatio()) > 0) {
            // Choose the first auction in the batch of last HOT_AUCTION_RATIO auctions.
            auction = (lastBase0AuctionId(eventId) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO;
        } else {
            auction = nextBase0AuctionId(eventId, random, config);
        }
        auction += GeneratorConfig.FIRST_AUCTION_ID;
//        long num1 = (dataGen.nextZipf(300, 0.6) - 1) * 100;
        // 300-500
//        auction = num1 + dataGen.nextZipf(100, 0.6);
        auction = nextLong(random, keys);
//        System.out.println("bid: " + auction);
        if(dataGen.nextInt(0, 2) == 1){
            auction = 100;
        }
//        auction = nextLong(random, 10000);

        long bidder;
        // Here P(bid will be by a hot bidder) = 1 - 1/hotBiddersRatio
        if (random.nextInt(config.getHotBiddersRatio()) > 0) {
            // Choose the second person (so hot bidders and hot sellers don't collide) in the batch of
            // last HOT_BIDDER_RATIO people.
            bidder = (lastBase0PersonId(eventId) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1;
        } else {
            bidder = nextBase0PersonId(eventId, random, config);
        }
        bidder += GeneratorConfig.FIRST_PERSON_ID;

        long price = PriceGenerator.nextPrice(random);
        int currentSize = 8 + 8 + 8 + 8;
        String extra = nextExtra(random, currentSize, config.getAvgBidByteSize());
        return new Bid(auction, bidder, price, new Instant(timestamp).getMillis(), extra);
    }

    /** Generate and return a random bid with next available id. */
    public Bid nextBidAucSkew(long eventId, Random random, long timestamp, GeneratorConfig config) {

        long auction;
        // To speed up the generation, we only skew the first 1000 keys. [1,1000]
        auction = dataGen.nextZipf(skewKeys, skewness);
        if (auction == skewKeys)
            auction = dataGen.nextLong(skewKeys, keys + 1);
//        auction = zipf.next();
//        if(dataGen.nextInt(0, 2) == 1){
//            auction = 100;
//        }

        long bidder;
        // Here P(bid will be by a hot bidder) = 1 - 1/hotBiddersRatio
        if (random.nextInt(config.getHotBiddersRatio()) > 0) {
            // Choose the second person (so hot bidders and hot sellers don't collide) in the batch of
            // last HOT_BIDDER_RATIO people.
            bidder = (lastBase0PersonId(eventId) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1;
        } else {
            bidder = nextBase0PersonId(eventId, random, config);
        }
        bidder += GeneratorConfig.FIRST_PERSON_ID;

        long price = PriceGenerator.nextPrice(random);
        int currentSize = 8 + 8 + 8 + 8;
        String extra = nextExtra(random, currentSize, config.getAvgBidByteSize());
        return new Bid(auction, bidder, price, new Instant(timestamp).getMillis(), extra);
    }

    /** Generate and return a random bid with next available id. */
    public Bid nextBidRandom(long eventId, Random random, long timestamp, GeneratorConfig config) {

        long auction = dataGen.nextLong(0, keys + 1);
//        auction = zipf.next();
//        if(dataGen.nextInt(0, 2) == 1){
//            auction = 100;
//        }

        long bidder;
        // Here P(bid will be by a hot bidder) = 1 - 1/hotBiddersRatio
        if (random.nextInt(config.getHotBiddersRatio()) > 0) {
            // Choose the second person (so hot bidders and hot sellers don't collide) in the batch of
            // last HOT_BIDDER_RATIO people.
            bidder = (lastBase0PersonId(eventId) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1;
        } else {
            bidder = nextBase0PersonId(eventId, random, config);
        }
        bidder += GeneratorConfig.FIRST_PERSON_ID;

        long price = PriceGenerator.nextPrice(random);
        int currentSize = 8 + 8 + 8 + 8;
        String extra = nextExtra(random, currentSize, config.getAvgBidByteSize());
        return new Bid(auction, bidder, price, new Instant(timestamp).getMillis(), extra);
    }
}
