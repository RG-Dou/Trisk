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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query20 {

    private static final Logger logger  = LoggerFactory.getLogger(Query3Stateful.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final int cpInterval = params.getInt("cp-interval", 100);
        env.enableCheckpointing(cpInterval);
        env.getConfig().setAutoWatermarkInterval(1);
        env.disableOperatorChaining();

        final int auctionSrcRate = params.getInt("auction-srcRate", 1000);
        final int bidSrcRate = params.getInt("bid-srcRate", 1000);
        final long stateSize = params.getLong("state-size", 1_000_000);
        final long keys = params.getLong("keys", 10000);
        final double skewness = params.getDouble("skewness", 1.0);
        final boolean inputRateSpy = params.getBoolean("input-spy", false);

        int warmUp = 2*180*1000;
        final boolean groupAll = params.getBoolean("group-all", false);
        String groupJoin = "join", groupFilter = "Filter";
        if (groupAll){
            groupJoin = "unified";
            groupFilter = "unified";
        }

        AuctionSourceFunction auctionSrc = new AuctionSourceFunction(auctionSrcRate, stateSize, keys, 0, warmUp);
        auctionSrc.setSkewField("Warmup");
        auctionSrc.setCategory(10);
        BidSourceFunction bidSrc = new BidSourceFunction(bidSrcRate, keys, skewness, warmUp);
        bidSrc.setSkewField("Auction");
        if (inputRateSpy) bidSrc.enableInputRateSpy();

        DataStream<Auction> auctions = env.addSource(auctionSrc)
                .name("Custom Source: Auctions")
                .setParallelism(params.getInt("p-auction-source", 1)).slotSharingGroup(groupJoin)
                .assignTimestampsAndWatermarks(new AuctionTimestampAssigner()).slotSharingGroup(groupJoin);

        DataStream<Bid> bids = env.addSource(bidSrc)
                .name("Custom Source: Bids")
                .setParallelism(params.getInt("p-bid-source", 1)).slotSharingGroup(groupJoin)
                .assignTimestampsAndWatermarks(new BidTimestampAssigner()).slotSharingGroup(groupJoin);

        // SELECT
        //      auction, bidder, price, channel, url, B.dataTime, B.extra,
        //      itemName, description, initialBid, reserve, A.dataTime, expires, seller, category, A.extra
        // FROM
        //      bid AS B INNER JOIN auction AS A on B.action = A.id
        // WHERE A.category = 10;

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

        DataStream<Tuple14<Long, Long, Long, Long, String, String, String, Long, Long, Long, Long, Long, Long, String>> joined = keyedAuctions.connect(keyedBids)
                .flatMap(new JoinBidsWithAuctions()).name("Incremental join").setParallelism(params.getInt("p-join", 1)).slotSharingGroup(groupJoin);

        DataStream<Tuple14<Long, Long, Long, Long, String, String, String, Long, Long, Long, Long, Long, Long, String>> flatMap = joined
                .filter(new FilterFunction<Tuple14<Long, Long, Long, Long, String, String, String, Long, Long, Long, Long, Long, Long, String>>() {
                    @Override
                    public boolean filter(Tuple14<Long, Long, Long, Long, String, String, String, Long, Long, Long, Long, Long, Long, String> joinTuple) throws Exception {
                        return joinTuple.f12 == 10;
                    }
                }).setParallelism(params.getInt("p-filter", 1)).slotSharingGroup(groupFilter);

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        flatMap.transform("Sink", objectTypeInfo, new DummyLatencyCountingSinkOutput<>(logger))
                .setParallelism(params.getInt("p-filter", 1)).slotSharingGroup(groupFilter);

        // execute program
        env.execute("Nexmark Query");
    }

    private static final class JoinBidsWithAuctions extends RichCoFlatMapFunction<Auction, Bid, Tuple14<Long, Long, Long, Long, String, String, String, Long, Long, Long, Long, Long, Long, String>> {

        // We only store auction message, since in practice, there should be an auction first, followed by bids
        private ValueState<Tuple9<String, String, Long, Long, Long, Long, Long, Long, String>> auctionMsg;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple9<String, String, Long, Long, Long, Long, Long, Long, String>> auctionDescriptor =
                    new ValueStateDescriptor<Tuple9<String, String, Long, Long, Long, Long, Long, Long, String>>(
                            "auction-msg",
                            TypeInformation.of(new TypeHint<Tuple9<String, String, Long, Long, Long, Long, Long, Long, String>>() {})
                    );
            auctionMsg = getRuntimeContext().getState(auctionDescriptor);
        }

        @Override
        public void flatMap1(Auction auction, Collector<Tuple14<Long, Long, Long, Long, String, String, String, Long, Long, Long, Long, Long, Long, String>> out) throws Exception {
            Tuple9<String, String, Long, Long, Long, Long, Long, Long, String> oldValue = auctionMsg.value();
            if (oldValue == null)
                oldValue = new Tuple9<>(auction.itemName, auction.description, auction.initialBid, auction.reserve, auction.dateTime, auction.expires, auction.seller, auction.category, auction.extra);
            else
                oldValue.f2 = auction.initialBid;
            auctionMsg.update(oldValue);
        }

        @Override
        public void flatMap2(Bid bid, Collector<Tuple14<Long, Long, Long, Long, String, String, String, Long, Long, Long, Long, Long, Long, String>> out) throws Exception {
            Tuple9<String, String, Long, Long, Long, Long, Long, Long, String> auction = auctionMsg.value();
//            if(bid.auction == 0)
//                delay(1_000_000);
            if(auction != null) {
                Tuple14<Long, Long, Long, Long, String, String, String, Long, Long, Long, Long, Long, Long, String> tuple =
                        new Tuple14<>(bid.auction, bid.bidder, bid.price, bid.dateTime, bid.extra,
                                auction.f0, auction.f1, auction.f2, auction.f3, auction.f4, auction.f5, auction.f6, auction.f7, auction.f8);
                out.collect(tuple);
            }
        }

        private void delay(int interval) {
            long start = System.nanoTime();
            while (System.nanoTime() - start < interval) {}
        }
    }

}
