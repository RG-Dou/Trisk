package Nexmark.sources.controllers;

public class BidSCRandomInputInc extends BidSCRandom{
    private long startTime;

    // example: every 1min (60 * 1000), increase 50.
    private final long window;
    private final int inc;

    public BidSCRandomInputInc(long keys, long window, int increment) {
        super(keys);
        this.window = window;
        this.inc = increment;
    }

    @Override
    public void beforeRun(){
        startTime = System.currentTimeMillis();
    }

    @Override
    public void checkAndAdjust(){
        if (System.currentTimeMillis() - startTime >= window){
            startTime = System.currentTimeMillis();
            function.setBase(function.getBase() + inc);
        }
    }
}
