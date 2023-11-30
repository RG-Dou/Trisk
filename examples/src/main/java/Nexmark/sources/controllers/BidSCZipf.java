package Nexmark.sources.controllers;

import Nexmark.sources.generator.model.ZipfUtil;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.PriceGenerator;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.joda.time.Instant;

import java.util.Random;

import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.lastBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.nextBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextExtra;

public class BidSCZipf extends BidSrcController{

    private final long keys;
    private final int skewKeys = 10;
    private final double skewness;

    private static final RandomDataGenerator dataGen = new RandomDataGenerator();

    public BidSCZipf(long keys, double skewness){
        this.keys = keys;
        this.skewness = skewness;
    }

    @Override
    public Bid nextBid(long eventId, Random random, long timestamp, GeneratorConfig config) {
        long auction;
        // To speed up the generation, we only skew the first 1000 keys. [1,1000]
        auction = dataGen.nextZipf(skewKeys, skewness);
        if (auction == skewKeys)
            auction = dataGen.nextLong(skewKeys, keys + 1);

        long bidder;
        // Here P(bid will be by a hot bidder) = 1 - 1/hotBiddersRatio
        if (random.nextInt(config.getHotBiddersRatio()) > 0) {
            // Choose the second person (so hot bidders and hot sellers don't collide) in the batch of
            // last HOT_BIDDER_RATIO people.
            bidder = (lastBase0PersonId(eventId) / getHotBidderRatio()) * getHotBidderRatio() + 1;
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
