package Nexmark.sources.controllers;

import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.PriceGenerator;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.lastBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.nextBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextExtra;

public class BidSCRandomSlideWin extends BidSrcController{

    // one window size is 0.5 min.
    private final long ONE_WIN = 30 * 1000;
    private final long SIZE = 10;
    private final long STEP = 1;

    private final long totalWindows;
    private final long keysPerStep;
    private final List<Long> keyPool= new ArrayList<>();

    private int currentIndex = 0;
    private long lastTime;
    private ArrayList<Integer> currentWindow = new ArrayList<>();

    private static final RandomDataGenerator dataGen = new RandomDataGenerator();

    public BidSCRandomSlideWin(long keys, int totalWindows) {
        this.totalWindows = totalWindows;
        this.keysPerStep = keys / (SIZE / STEP);

        System.out.println("key per window size is: " + keysPerStep);
        for (long i = 0; i < totalWindows; i ++){
            keyPool.add(keysPerStep * i);
        }
    }

    @Override
    public void beforeRun(){
        lastTime = System.currentTimeMillis();
        currentWindow.add(currentIndex);
    }

    @Override
    public void checkAndAdjust(){
        long now = System.currentTimeMillis();
        if (now - lastTime > ONE_WIN){
            currentIndex += 1;
            currentIndex %= totalWindows;
            currentWindow.add(currentIndex);
            if (currentWindow.size() > SIZE){
                currentWindow.remove(0);
            }
        }
    }

    @Override
    public Bid nextBid(long eventId, Random random, long timestamp, GeneratorConfig config) {
        System.out.println("Win:" + currentWindow);
        int randomIndex = currentWindow.get(dataGen.nextInt(0, currentWindow.size() - 1));
        long start_id = keyPool.get(randomIndex);
        long auction = dataGen.nextLong(start_id, start_id + keysPerStep);

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
