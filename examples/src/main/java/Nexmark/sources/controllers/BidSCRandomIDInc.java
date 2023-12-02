package Nexmark.sources.controllers;

import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.PriceGenerator;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.joda.time.Instant;

import java.util.Random;

import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.lastBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.nextBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextExtra;

public class BidSCRandomIDInc extends BidSrcController {

    private final long total_keys;
    private final long key_length;
    private final double probability;

    private long start_id;

    private static final RandomDataGenerator dataGen = new RandomDataGenerator();

    public BidSCRandomIDInc(long total_keys, long key_length, double probability){
        this.total_keys = total_keys;
        this.key_length = key_length;
        this.probability = probability;
        start_id = 0;
    }

    @Override
    public void checkAndAdjust(){
        System.out.println("start ID: " + start_id);
    }

    @Override
    public Bid nextBid(long eventId, Random random, long timestamp, GeneratorConfig config) {
        double randomValue = dataGen.nextUniform(0, 1);
        if (randomValue < probability)
            start_id += 1;

        long auction = dataGen.nextLong(start_id, start_id + key_length);

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