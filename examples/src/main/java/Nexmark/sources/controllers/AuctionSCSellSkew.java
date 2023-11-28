package Nexmark.sources.controllers;

import Nexmark.sources.AuctionSourceFunction;

public class AuctionSCSellSkew extends AuctionSrcController {

    private long index = 0;
    private long parallel = 1;
    private long nextID = 0;

    private final long keys;

    public AuctionSCSellSkew(long keys) {
        this.keys = keys;
    }

    @Override
    public void beforeRun(){
        index = srcFunction.getRuntimeContext().getIndexOfThisSubtask();
        parallel = srcFunction.getRuntimeContext().getNumberOfParallelSubtasks();
        srcFunction.setBase(500000);
    }

    @Override
    public void checkAndAdjust(){
        if(nextID >= keys / parallel) {
            srcFunction.setBase(0);
        }
    }

    @Override
    public long nextAuctionId(long eventId){
        long id = nextID * parallel + index;
        nextID ++;
        return id;
    }
}
