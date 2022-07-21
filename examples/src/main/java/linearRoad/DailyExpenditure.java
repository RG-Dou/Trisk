package linearRoad;

import Nexmark.sinks.DummyLatencyCountingSinkOutput;
import linearRoad.data.ExpenditureData;
import linearRoad.data.ExpenditurePool;
import linearRoad.source.DERequestOrHistData;
import linearRoad.source.DailyExpenditureHistData;
import linearRoad.source.DailyExpenditureRequest;
import linearRoad.source.RequestSourceFuntion;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class DailyExpenditure {
    private static final Logger logger  = LoggerFactory.getLogger(DailyExpenditure.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.getConfig().setAutoWatermarkInterval(1);
        env.disableOperatorChaining();

        final int requestRate = params.getInt("request-rate", 1000);
        final String histFilePath = params.get("hist-file", "/data/EMM_data/histData/");

        final int stateSize = params.getInt("state-size", 1_00_000);
        final double skewness = params.getDouble("skewness", 1.0);
//
//        int warmUp = 2*180*1000;
        final boolean groupAll = params.getBoolean("group-all", false);
        String groupJoin = "join", groupFilter = "Filter";
        if (groupAll){
            groupJoin = "unified";
            groupFilter = "unified";
        }

        long keys = 10;
        RequestSourceFuntion requestSrc = new RequestSourceFuntion(requestRate, keys);
        requestSrc.setFilePath(histFilePath);

        DataStream<DERequestOrHistData> requests = env.addSource(requestSrc)
                .name("Custom Source: Request")
                .setParallelism(params.getInt("p-state", 1)).slotSharingGroup(groupJoin)
                .assignTimestampsAndWatermarks(new RequestTimestampAssigner()).slotSharingGroup(groupJoin);

        // SELECT
        //      auction, bidder, price, channel, url, B.dataTime, B.extra,
        //      itemName, description, initialBid, reserve, A.dataTime, expires, seller, category, A.extra
        // FROM
        //      bid AS B INNER JOIN auction AS A on B.action = A.id
        // WHERE A.category = 10;

        KeyedStream<DERequestOrHistData, Integer> keyedRequest =
                requests.keyBy(new KeySelector<DERequestOrHistData, Integer>() {
                    @Override
                    public Integer getKey(DERequestOrHistData request) throws Exception {
                        return request.getXway();
                    }
                });

        DataStream<Tuple5<Integer, Long, Long, Integer, Integer>> find = keyedRequest
                .flatMap(new ProcessRequest(stateSize)).name("Incremental join").setParallelism(params.getInt("p-state", 1)).slotSharingGroup(groupJoin);

        DataStream<Tuple5<Integer, Long, Long, Integer, Integer>> flatMap = find
                .filter(new FilterFunction<Tuple5<Integer, Long, Long, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple5<Integer, Long, Long, Integer, Integer> joinTuple) throws Exception {
                        return (joinTuple.f2 - joinTuple.f1 < 10_000);
                    }
                }).setParallelism(params.getInt("p-filter", 1)).slotSharingGroup(groupFilter);

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        flatMap.transform("Sink", objectTypeInfo, new DummyLatencyCountingSinkOutput<>(logger))
                .setParallelism(params.getInt("p-filter", 1)).slotSharingGroup(groupFilter);

        // execute program
        env.execute("Nexmark Query");
    }

    private static final class ProcessRequest extends RichFlatMapFunction<DERequestOrHistData, Tuple5<Integer, Long, Long, Integer, Integer>> {

        // We only store auction message, since in practice, there should be an auction first, followed by bids
//        map<Day, ExpenditureData>
        private MapState<Integer, ExpenditurePool> historicalState;
        private int extraSize;
        private final int INDEX = 3;

        public ProcessRequest(int extraSize){
            this.extraSize = extraSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Integer, ExpenditurePool> stateDescriptor =
                    new MapStateDescriptor<>("hist-state", TypeInformation.of(new TypeHint<Integer>() {}), TypeInformation.of(new TypeHint<ExpenditurePool>() {}));
            historicalState = getRuntimeContext().getMapState(stateDescriptor);
        }

        private Tuple5<Integer, Long, Long, Integer, Integer> loadHistoriclData(DailyExpenditureHistData tuple) throws Exception {
            int xway = tuple.getXway();
            int day = tuple.getDay();
            int vid = tuple.getVid();
            int toll = tuple.getToll();
            ExpenditurePool pool = historicalState.get(day);
            if(pool == null){
                pool = new ExpenditurePool();
            }
            ExpenditureData data = new ExpenditureData(xway, day, vid, toll, extraSize);
            pool.addData(data);
            historicalState.put(day, pool);
            return new Tuple5<>(INDEX, 0L, 0L, 0, 0);
        }

        private Tuple5<Integer, Long, Long, Integer, Integer> processRequest(DailyExpenditureRequest request) throws Exception {

            int day = request.getDay();
            int vid = request.getVid();
            int totalToll = 0;
            ExpenditurePool pool = this.historicalState.get(day);
            if (pool != null) {
                for (ExpenditureData data : pool.lists) {
                    if (data.getVid() == vid) {
                        totalToll += data.getToll();
                    }
                }
            }
            return new Tuple5<>(INDEX, request.getTimestamp(), System.currentTimeMillis(), vid, totalToll);
        }

        @Override
        public void flatMap(DERequestOrHistData request, Collector<Tuple5<Integer, Long, Long, Integer, Integer>> out) throws Exception {
            if(!request.isRequest()){
                out.collect(loadHistoriclData((DailyExpenditureHistData) request));
            }
            else {
                out.collect(processRequest((DailyExpenditureRequest) request));
            }
        }

        private void delay(int interval) {
            long start = System.nanoTime();
            while (System.nanoTime() - start < interval) {}
        }
    }

}
