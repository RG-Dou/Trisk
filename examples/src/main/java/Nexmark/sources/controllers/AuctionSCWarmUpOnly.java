package Nexmark.sources.controllers;

import Nexmark.sources.AuctionSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.lastBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.nextBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PriceGenerator.nextPrice;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextExtra;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextString;

import org.joda.time.Instant;
import java.util.Random;

public class AuctionSCWarmUpOnly extends AuctionSrcController {

    private long index = 0;
    private long parallel = 1;
    private long nextID = 0;

    private final long keys;
    private long startTime;

    public AuctionSCWarmUpOnly(long keys) {
        this.keys = keys;
    }

    @Override
    public void beforeRun(){
        index = srcFunction.getRuntimeContext().getIndexOfThisSubtask();
        parallel = srcFunction.getRuntimeContext().getNumberOfParallelSubtasks();
        srcFunction.setBase(500000);
        startTime = System.currentTimeMillis();
    }

    @Override
    public void checkAndAdjust(){
        if(nextID >= keys / parallel) {
            System.out.println("Stop warm up, total time is: " + (System.currentTimeMillis() - startTime) + "ms.");
            srcFunction.cancel();
        }
    }

    @Override
    public Auction nextAuction(
            long eventsCountSoFar, long eventId, Random random, long timestamp, GeneratorConfig config) {

        // generate id sequentially
        long id = (nextID * parallel) + index;
        nextID += 1;

        long seller;
        // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
        if (random.nextInt(config.getHotSellersRatio()) > 0) {
            // Choose the first person in the batch of last HOT_SELLER_RATIO people.
            seller = (lastBase0PersonId(eventId) / getHotSellerRatio()) * getHotSellerRatio();
        } else {
            seller = nextBase0PersonId(eventId, random, config);
        }
        seller += GeneratorConfig.FIRST_PERSON_ID;

        long category = GeneratorConfig.FIRST_CATEGORY_ID + random.nextInt(getNUM_CATEGORIES());

        // modify the initialBid and the expires.
        long initialBid = 100_000_000;
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
}
