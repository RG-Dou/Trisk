package Nexmark.sources.controllers;

import Nexmark.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.PriceGenerator;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Random;

import static org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator.lastBase0AuctionId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator.nextBase0AuctionId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.lastBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.nextBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextExtra;

public class BidSrcController implements Serializable {

    /**
     * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
     * values.
     */
    private static final int HOT_AUCTION_RATIO = 100;

    private static final int HOT_BIDDER_RATIO = 100;

    public BidSourceFunction function;

    public BidSrcController(){}

    public void beforeRun(){}

    public void checkAndAdjust(){}

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

    public int getHotAuctionRatio(){
        return HOT_AUCTION_RATIO;
    }

    public int getHotBidderRatio(){
        return HOT_BIDDER_RATIO;
    }
}
