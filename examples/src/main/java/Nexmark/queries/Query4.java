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

package Nexmark.queries;

import Nexmark.sinks.DummyLatencyCountingSinkOutput;
import Nexmark.sources.AuctionSourceFunction;
import Nexmark.sources.BidSourceFunction;
import Nexmark.windowing.*;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query4 {

    private static final Logger logger  = LoggerFactory.getLogger(Query3Stateful.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(20);
        env.getConfig().setAutoWatermarkInterval(1);
        env.disableOperatorChaining();

        final int auctionSrcRate = params.getInt("auction-srcRate", 20000);
        final int bidSrcRate = params.getInt("bid-srcRate", 10000);
        final long stateSize = params.getLong("state-size", 20);

        DataStream<Auction> auctions = env.addSource(new AuctionSourceFunction(auctionSrcRate, stateSize))
                .name("Custom Source: Auctions")
                .setParallelism(params.getInt("p-auction-source", 1)).slotSharingGroup("source")
                .assignTimestampsAndWatermarks(new AuctionTimestampAssigner()).slotSharingGroup("source");

        DataStream<Bid> bids = env.addSource(new BidSourceFunction(bidSrcRate))
                .name("Custom Source: Bids")
                .setParallelism(params.getInt("p-bid-source", 1)).slotSharingGroup("source")
                .assignTimestampsAndWatermarks(new BidTimestampAssigner()).slotSharingGroup("source");

        // SELECT
        //      Q.category
        //      AVG(Q.final)
        // FROM (
        //      SELECT MAX(B.price) AS final, A.category
        //      FROM auction A, bid B
        //      WHERE A.id = B.auction AND B.dataTime BETWEEN A.dataTime AND A.expires
        //      GROUP BY A.id, A.category
        // ) Q
        // GROUP BY Q.category;

        KeyedStream<Auction, Long> keyedAuctions =
                auctions.keyBy(new KeySelector<Auction, Long>() {
                    @Override
                    public Long getKey(Auction auction) throws Exception {
                        return auction.id;
                    }
                });

        KeyedStream<Bid, Long> keyedBids =
                bids.keyBy(new KeySelector<Bid, Long>() {
                    @Override
                    public Long getKey(Bid bid) throws Exception {
                        return bid.auction;
                    }
                });

        DataStream<Tuple2<Long, Long>> joined = keyedAuctions.connect(keyedBids)
                .flatMap(new JoinBidsWithAuctions()).name("Incremental join").setParallelism(params.getInt("p-join", 1)).slotSharingGroup("join");


        DataStream<Double> window = joined.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
                    public Long getKey(Tuple2<Long, Long> tuple) throws Exception{
//                        long start = System.nanoTime();
//                        while (System.nanoTime() - start < 100_000) {}
                        return tuple.f0;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .trigger(new DummyTrigger())
                .aggregate(new AvgAgg())
                .name("Sliding Window")
                .setParallelism(params.getInt("p-window", 1)).slotSharingGroup("sink");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        window.transform("Sink", objectTypeInfo, new DummyLatencyCountingSinkOutput<>(logger))
                .setParallelism(params.getInt("p-sink", 1)).slotSharingGroup("sink");

        // execute program
        env.execute("Nexmark Query");
    }

    private static final class JoinBidsWithAuctions extends RichCoFlatMapFunction<Auction, Bid, Tuple2<Long, Long>> {

        // We only store auction message, since in practice, there should be an auction first, followed by bids
        private ValueState<Tuple6<Long, Long, Long, Long, Long, String>> auctionMsg;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple6<Long, Long, Long, Long, Long, String>> auctionDescriptor =
                    new ValueStateDescriptor<Tuple6<Long, Long, Long, Long, Long, String>>(
                            "auction-msg",
                            TypeInformation.of(new TypeHint<Tuple6<Long, Long, Long, Long, Long, String>>() {})
                    );
            auctionMsg = getRuntimeContext().getState(auctionDescriptor);
        }

        @Override
        public void flatMap1(Auction auction, Collector<Tuple2<Long, Long>> out) throws Exception {
            Tuple6<Long, Long, Long, Long, Long, String> oldValue = auctionMsg.value();
            if (oldValue == null) {
                Tuple6<Long, Long, Long, Long, Long, String> tuple =
                        new Tuple6<>(auction.id, auction.category, auction.initialBid, auction.dateTime, auction.expires, auction.extra);
                auctionMsg.update(tuple);
            } else if (auction.dateTime > oldValue.f4){
                //the last auction is closed, output the max price and category.
                Tuple2<Long, Long> maxPrice = new Tuple2<>(oldValue.f1, oldValue.f2);
                out.collect(maxPrice);

                Tuple6<Long, Long, Long, Long, Long, String> tuple =
                        new Tuple6<>(auction.id, auction.category, auction.initialBid, auction.dateTime, auction.expires, auction.extra);
                auctionMsg.update(tuple);
            }
        }

        @Override
        public void flatMap2(Bid bid, Collector<Tuple2<Long, Long>> out) throws Exception {
            Tuple6<Long, Long, Long, Long, Long, String> auction = auctionMsg.value();
            if(auction != null){
                if(bid.dateTime > auction.f3 && bid.dateTime < auction.f4 && bid.price > auction.f2){
                    auction.f2 = bid.price;
                    auctionMsg.update(auction);
                } else if (bid.dateTime > auction.f4){
                    Tuple2<Long, Long> maxPrice = new Tuple2<>(auction.f1, auction.f2);
                    out.collect(maxPrice);
                    auctionMsg.clear();
                }
            }
        }

        private void delay(int interval) {
            long start = System.nanoTime();
            while (System.nanoTime() - start < interval) {}
        }
    }

    private static class AvgAgg
            implements AggregateFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Double> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<Long, Long> value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return ((double) accumulator.f0) / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

}
