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

import Nexmark.sinks.DummyLatencyCountingSink;
import Nexmark.sources.AuctionSourceFunction;
import Nexmark.sources.PersonSourceFunction;
import Nexmark.sources.controllers.AuctionSrcController;
import Nexmark.windowing.*;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class Query3StatefulWindow {

private static final Logger logger  = LoggerFactory.getLogger(Query3StatefulWindow.class);

public static void main(String[] args) throws Exception {

    // Checking input parameters
    final ParameterTool params = ParameterTool.fromArgs(args);

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // enable latency tracking
    env.getConfig().setLatencyTrackingInterval(1000);

//    env.enableCheckpointing(1000);
    env.getConfig().setAutoWatermarkInterval(1);

    env.disableOperatorChaining();

    final int auctionSrcRate = params.getInt("auction-srcRate", 20000);

    final int personSrcRate = params.getInt("person-srcRate", 10000);

    final long stateSize = params.getLong("state-size", 20);

    DataStream<Auction> auctions = env.addSource(new AuctionSourceFunction(auctionSrcRate, stateSize, new AuctionSrcController()))
            .name("Custom Source: Auctions")
            .setParallelism(params.getInt("p-auction-source", 1)).slotSharingGroup("source")
            .assignTimestampsAndWatermarks(new AuctionTimestampAssigner()).slotSharingGroup("source");

    DataStream<Person> persons = env.addSource(new PersonSourceFunction(personSrcRate, stateSize))
            .name("Custom Source: Persons")
            .setParallelism(params.getInt("p-person-source", 1)).slotSharingGroup("source")
//            .filter(new FilterFunction<Person>() {
//                @Override
//                public boolean filter(Person person) throws Exception {
//                    return (person.state.equals("OR") || person.state.equals("ID") || person.state.equals("CA"));
//                }
//            })
//            .setParallelism(params.getInt("p-person-source", 1)).slotSharingGroup("source")
            .assignTimestampsAndWatermarks(new PersonTimestampAssigner()).slotSharingGroup("source");

    // SELECT Istream(P.name, P.city, P.state, A.id)
    // FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
    // WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA')

  KeyedStream<Auction, Long> keyedAuctions =
          auctions.keyBy(new KeySelector<Auction, Long>() {
             @Override
             public Long getKey(Auction auction) throws Exception {
                return auction.seller;
             }
          });

  KeyedStream<Person, Long> keyedPersons =
            persons.keyBy(new KeySelector<Person, Long>() {
                @Override
                public Long getKey(Person person) throws Exception {
                    return person.id;
                }
            });

  DataStream<Tuple4<String, String, String, Long>> joined = keyedAuctions.connect(keyedPersons)
          .flatMap(new JoinPersonsWithAuctions()).name("Incremental join").setParallelism(params.getInt("p-join", 1)).slotSharingGroup("join");

    DataStream<Long> window = joined.keyBy(new KeySelector<Tuple4<String, String, String, Long>, Long>() {
        public Long getKey(Tuple4<String, String, String, Long> tuple4) throws Exception{
            long start = System.nanoTime();
            while (System.nanoTime() - start < 100_000) {}
            return tuple4.f3;
        }
    }).window(TumblingEventTimeWindows.of(Time.seconds(1)))
            .trigger(new DummyTrigger())
            .aggregate(new CountResult())
            .name("Sliding Window")
            .setParallelism(params.getInt("p-window", 1)).slotSharingGroup("sink");

    GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
    window.transform("Sink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
            .setParallelism(params.getInt("p-sink", 1)).slotSharingGroup("sink");

    // execute program
    env.execute("Nexmark Query3 stateful");
}

private static final class CountResult implements AggregateFunction<Tuple4<String, String, String, Long>, Long, Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple4<String, String, String, Long> value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}


private static final class JoinPersonsWithAuctions extends RichCoFlatMapFunction<Auction, Person, Tuple4<String, String, String, Long>> {

    // person state: id, <name, city, state>
    private MapState<Long, Tuple3<String, String, String>> personMap;

    // auction state: seller, List<id>
//    private HashMap<Long, HashSet<Long>> auctionMap = new HashMap<>();
//    private MapState<Long, HashSet<Long>> auctionMap;
    private RandomDataGenerator randomGen = new RandomDataGenerator();
    private Random random = new Random();

    private final int readCounter = 15;
    private final int writeCounter = 30;
    private int taskIndex;
    private String extra;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Long, Tuple3<String, String, String>> personDescriptor =
                new MapStateDescriptor<Long, Tuple3<String, String, String>>(
                        "person-map",
                        BasicTypeInfo.LONG_TYPE_INFO,
                        new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
                       );
        MapStateDescriptor<Long, HashSet<Long>> auctionDescriptor =
                new MapStateDescriptor<Long, HashSet<Long>>(
                        "auction-map",
                        TypeInformation.of(new TypeHint<Long>() {}),
                        TypeInformation.of(new TypeHint<HashSet<Long>>() {})
                );
        taskIndex = getRuntimeContext().getIndexOfThisSubtask();

        personMap = getRuntimeContext().getMapState(personDescriptor);
//        auctionMap = getRuntimeContext().getMapState(auctionDescriptor);
        extra = randomGen.nextHexString(10);
    }

    @Override
    public void flatMap1(Auction auction, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
        // check if auction has a match in the person state
//        long startTime = System.currentTimeMillis();
//        long start = System.nanoTime();
//        long subStruct = 0;
        if (personMap.contains(auction.seller)) {
            // emit and don't store
            Tuple3<String, String, String> match = personMap.get(auction.seller);
            if(random.nextInt(10) == 0)
                out.collect(new Tuple4<>(match.f0, match.f1, match.f1, auction.id));
        }
//        System.out.println("ts: " + startTime + " endToEnd Latency1: " + ((System.nanoTime() - start - subStruct) / 1000000.0));
//        out.collect(new Tuple4<>(auction.description, auction.itemName, auction.itemName, auction.id));
//        for (long i = 0; i < readCounter; i ++){
//            long key = new Random().nextInt(writeCounter);
//            if(personMap.contains(key)){
//                Tuple3<String, String, String> match = personMap.get(key);
//                out.collect(new Tuple4<>(match.f0, match.f1, match.f1, auction.id));
//            }
//        }
//        if(taskIndex == 0)
            delay(500_000);
//        else
//            delay(1_000_000);
//        delay(1_000_000);
//        long start = System.nanoTime();
//        System.out.println("flap map 1: " + ((System.nanoTime() - start) / 1000000.0));
//        else {
//            // we need to store this auction for future matches
//            if (auctionMap.containsKey(auction.seller)) {
//                HashSet<Long> ids = auctionMap.get(auction.seller);
//                ids.add(auction.id);
//                auctionMap.put(auction.seller, ids);
//            }
//            else {
//                HashSet<Long> ids = new HashSet<>();
//                ids.add(auction.id);
//                auctionMap.put(auction.seller, ids);
//            }
//        }
//        metricsDump();
//        delay 0.1ms
//        delay(100_000);
    }

    @Override
    public void flatMap2(Person person, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
        // store person in state
//        long start = System.nanoTime();
//        Tuple3<String, String, String> value = new Tuple3<>(person.name, person.city, person.state);
        if(!personMap.contains(person.id)) {
            personMap.put(person.id, new Tuple3<>(person.name, person.city, extra));
        }

//        if(!personMap.contains(1L)){
//            for(long i = 0; i < writeCounter; i ++){
//                personMap.put(i, new Tuple3<>(person.name, person.city, person.extra));
//            }
//        }
//        System.out.println("flap map 2: " + ((System.nanoTime() - start) / 1000000.0));

//        // check if person has a match in the auction state
//        if (auctionMap.containsKey(person.id)) {
//            // output all matches and remove
//            HashSet<Long> auctionIds = auctionMap.remove(person.id);
//            for (Long auctionId : auctionIds) {
//                out.collect(new Tuple4<>(person.name, person.city, person.state, auctionId));
//            }
//        }
        //delay 0.1ms
//        delay(100_000);
//        if(taskIndex == 0)
//            delay(2_000_000);
    }

    public void metricsDump() throws Exception {
        Iterator iter = personMap.iterator();
        int sumTest = 0;
        while (iter.hasNext()) {
            Map.Entry<Long, Tuple3<String, String, String>> entry = (Map.Entry<Long, Tuple3<String, String, String>>) iter.next();
            sumTest++;
        }

        System.out.println("test size: " + sumTest);
    }
    private void delay(int interval) {
        long start = System.nanoTime();
        while (System.nanoTime() - start < interval) {}
    }
}

}
