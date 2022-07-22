package linearRoad.source;

import Nexmark.sources.Util;
import linearRoad.data.ExpenditureData;
import linearRoad.data.ExpenditurePool;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;
import java.util.Random;

public class RequestSourceFuntion extends RichParallelSourceFunction<DERequestOrHistData> {

    private volatile boolean running = true;
    //    private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
    private GeneratorConfig config;
    private long eventsCountSoFar = 0;
    private int rate;
    private int cycle = 60;
    private int base = 0;
    private int warmUpInterval = 60000;
    private int densityId = 25;
    private double skewness;
    private long skewKeys;
    private String filePath;
    private Boolean inputRateSpy = false;

    private int qidCurrent = 0;
    private static final RandomDataGenerator dataGen = new RandomDataGenerator();

    public RequestSourceFuntion(int srcRate) {
        this(srcRate, 0);
    }

    public RequestSourceFuntion(int srcRate, int base) {
        this(srcRate, base, 60);
    }

    public RequestSourceFuntion(int srcRate, int base, int cycle) {
        this(srcRate, base, cycle, 40000);
    }

    public RequestSourceFuntion(int srcRate, int base, int cycle, int warmUpInterval) {
        this(srcRate, base, cycle, warmUpInterval, 1.0, 100000);
    }
    public RequestSourceFuntion(int base, long keys){
        this(base, keys, 1.0);
    }

    public RequestSourceFuntion(int base, long keys, double skewness){
        this(base, keys, skewness, 10000);
    }

    public RequestSourceFuntion(int base, long keys, double skewness, int warmUpInterval){
        this(0, base, 60, warmUpInterval, skewness, keys);
    }

    public RequestSourceFuntion(int srcRate, int base, int cycle, int warmUpInterval, double skewness, long keys) {
        this.rate = srcRate;
        this.cycle = cycle;
        this.base = base;
        this.warmUpInterval = warmUpInterval;
        this.skewness = skewness;
        this.skewKeys = keys;
        NexmarkConfiguration nexConfig = NexmarkConfiguration.DEFAULT;
        config = new GeneratorConfig(nexConfig, 1, 1000L, 0, 1);
    }

    @Override
    public void run(SourceContext<DERequestOrHistData> ctx) throws Exception {
        System.out.println("start run source");

        int epoch = 0;
        int count = 0;
        int curRate = rate;

        // warm up
        long now = System.currentTimeMillis();
        long newStartTs = now;
        if(getRuntimeContext().getIndexOfThisSubtask() == 0)
            sendHistoricalData(ctx);
        long duration = System.currentTimeMillis() - now;
        System.out.println("send historical data, duration: " + duration + ", number of events: " + eventsCountSoFar);
        Thread.sleep(400000 - duration);
        warmup(ctx);
        System.out.println("number of events: " + eventsCountSoFar);

        while (running) {
            long emitStartTime = System.currentTimeMillis();

            if (count == 20) {
                // change input rate every 1 second.
                epoch++;
                curRate = base + Util.changeRateSin(rate, cycle, epoch);
                System.out.println("Request: epoch: " + epoch%cycle + " current rate is: " + curRate);
                count = 0;
            }

            sendEvents(ctx, curRate, "");

            if(inputRateSpy) {
                if(System.currentTimeMillis() - newStartTs >= 60*1000){
                    newStartTs = System.currentTimeMillis();
                    base = base + 50;
                }
            }

            // Sleep for the rest of timeslice if needed
            Util.pause(emitStartTime);
            count++;
        }
    }

    private void sendHistoricalData(SourceContext<DERequestOrHistData> ctx){
        BufferedReader in;
        try {
            in = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = in.readLine()) != null) {

                String[] fields = line.split(" ");
                fields[0] = fields[0].substring(2);
                fields[3] = fields[3].substring(0, fields[3].length() - 1);
                int xway = Integer.parseInt(fields[0]);
                int day = Integer.parseInt(fields[1]);
                int vid = Integer.parseInt(fields[2]);
                int toll = Integer.parseInt(fields[3]);
                DailyExpenditureHistData data = new DailyExpenditureHistData(vid, xway, qidCurrent, day, toll);
                ctx.collect(data);
                eventsCountSoFar++;
                Thread.sleep(5);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void warmup(SourceContext<DERequestOrHistData> ctx) throws InterruptedException {
        int curRate = rate + base; //  (sin0 + 1) * rate + base
        curRate = 100;
        long startTs = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTs < warmUpInterval) {
            long emitStartTime = System.currentTimeMillis();

            sendEvents(ctx, curRate, "Warmup");
            // Sleep for the rest of timeslice if needed
            Util.pause(emitStartTime);
        }
    }

    private void sendEvents(SourceContext<DERequestOrHistData> ctx, int curRate, String field){

        for (int i = 0; i < curRate / 20; i++) {
            DERequestOrHistData request;
            if(Objects.equals(field, "Warmup")){
                request = nextRequestRandom();
            } else {
                request = nextRequestSkew();
            }

            ctx.collect(request);
            eventsCountSoFar++;
        }
    }

    private DailyExpenditureRequest nextRequestRandom(){
        long timestamp = System.currentTimeMillis();
        int vid = new Random().nextInt(2);
        int day = new Random().nextInt(70);
        int qid = qidCurrent;
        qidCurrent += 1;
        int xway = new Random().nextInt(580);
        return new DailyExpenditureRequest(timestamp, vid, xway, qid, day);
    }

    private DailyExpenditureRequest nextRequestSkew(){
        long timestamp = System.currentTimeMillis();
        int qid = qidCurrent;
        qidCurrent += 1;

        int xway = dataGen.nextZipf((int) skewKeys, skewness);
        if (xway == skewKeys)
            xway = dataGen.nextInt((int) skewKeys, 580);
        int vid = new Random().nextInt(2);
        int day = new Random().nextInt(70);
        return new DailyExpenditureRequest(timestamp, vid, xway, qid, day);
    }

    @Override
    public void cancel() {
        running = false;
    }

    private long nextId() {
        return (config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar)) * densityId;
    }

    public void setFilePath(String filePath){
        this.filePath = filePath;
    }

    public void enableInputRateSpy(){
        this.inputRateSpy = true;
    }

}
