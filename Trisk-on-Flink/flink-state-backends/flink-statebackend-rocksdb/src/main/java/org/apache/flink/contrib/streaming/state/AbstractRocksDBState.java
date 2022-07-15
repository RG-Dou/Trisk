/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class for {@link State} implementations that store state in a RocksDB database.
 *
 * <p>State is not stored in this class but in the {@link org.rocksdb.RocksDB} instance that
 * the {@link RocksDBStateBackend} manages and checkpoints.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of values kept internally in state.
 */
public abstract class AbstractRocksDBState<K, N, V> implements InternalKvState<K, N, V>, State {

	/** Serializer for the namespace. */
	final TypeSerializer<N> namespaceSerializer;

	/** Serializer for the state values. */
	final TypeSerializer<V> valueSerializer;

	/** The current namespace, which the next value methods will refer to. */
	private N currentNamespace;

	/** Backend that holds the actual RocksDB instance where we store state. */
	protected RocksDBKeyedStateBackend<K> backend;

	/** The column family of this particular instance of state. */
	protected ColumnFamilyHandle columnFamily;

	protected final V defaultValue;

	protected final WriteOptions writeOptions;

	protected final DataOutputSerializer dataOutputView;

	protected final DataInputDeserializer dataInputView;

	private final RocksDBSerializedCompositeKeyBuilder<K> sharedKeyNamespaceSerializer;

	private final PutStatsMetrics<K> putStatsMetrics;

	/**
	 * Creates a new RocksDB backed state.
	 *
	 * @param columnFamily The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param defaultValue The default value for the state.
	 * @param backend The backend for which this state is bind to.
	 */
	protected AbstractRocksDBState(
			ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> valueSerializer,
			V defaultValue,
			String stateName,
			MetricGroup metricGroup,
			RocksDBKeyedStateBackend<K> backend) {

		this.namespaceSerializer = namespaceSerializer;
		this.backend = backend;

		this.columnFamily = columnFamily;

		this.writeOptions = backend.getWriteOptions();
		this.valueSerializer = Preconditions.checkNotNull(valueSerializer, "State value serializer");
		this.defaultValue = defaultValue;

		this.dataOutputView = new DataOutputSerializer(128);
		this.dataInputView = new DataInputDeserializer();
		this.sharedKeyNamespaceSerializer = backend.getSharedRocksKeyBuilder();

		this.putStatsMetrics = new PutStatsMetrics<K>(metricGroup, stateName, backend.dbOptions);
	}

	// ------------------------------------------------------------------------

	@Override
	public void clear() {
		try {
			backend.db.delete(columnFamily, writeOptions, serializeCurrentKeyWithGroupAndNamespace());
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while removing entry from RocksDB", e);
		}
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.currentNamespace = namespace;
	}

	@Override
	public byte[] getSerializedValue(
			final byte[] serializedKeyAndNamespace,
			final TypeSerializer<K> safeKeySerializer,
			final TypeSerializer<N> safeNamespaceSerializer,
			final TypeSerializer<V> safeValueSerializer) throws Exception {

		//TODO make KvStateSerializer key-group aware to save this round trip and key-group computation
		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
				serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(keyAndNamespace.f0, backend.getNumberOfKeyGroups());

		RocksDBSerializedCompositeKeyBuilder<K> keyBuilder =
						new RocksDBSerializedCompositeKeyBuilder<>(
							safeKeySerializer,
							backend.getKeyGroupPrefixBytes(),
							32
						);
		keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);
		byte[] key = keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);
		return backend.db.get(columnFamily, key);
	}

	<UK> byte[] serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
		UK userKey,
		TypeSerializer<UK> userKeySerializer) throws IOException {
		return sharedKeyNamespaceSerializer.buildCompositeKeyNamesSpaceUserKey(
			currentNamespace,
			namespaceSerializer,
			userKey,
			userKeySerializer
		);
	}

	private <T> byte[] serializeValueInternal(T value, TypeSerializer<T> serializer) throws IOException {
		serializer.serialize(value, dataOutputView);
		return dataOutputView.getCopyOfBuffer();
	}

	byte[] serializeCurrentKeyWithGroupAndNamespace() {
		return sharedKeyNamespaceSerializer.buildCompositeKeyNamespace(currentNamespace, namespaceSerializer);
	}

	byte[] serializeValue(V value) throws IOException {
		return serializeValue(value, valueSerializer);
	}

	<T> byte[] serializeValueNullSensitive(T value, TypeSerializer<T> serializer) throws IOException {
		dataOutputView.clear();
		dataOutputView.writeBoolean(value == null);
		return serializeValueInternal(value, serializer);
	}

	<T> byte[] serializeValue(T value, TypeSerializer<T> serializer) throws IOException {
		dataOutputView.clear();
		return serializeValueInternal(value, serializer);
	}

	<T> byte[] serializeValueList(
		List<T> valueList,
		TypeSerializer<T> elementSerializer,
		byte delimiter) throws IOException {

		dataOutputView.clear();
		boolean first = true;

		for (T value : valueList) {
			Preconditions.checkNotNull(value, "You cannot add null to a value list.");

			if (first) {
				first = false;
			} else {
				dataOutputView.write(delimiter);
			}
			elementSerializer.serialize(value, dataOutputView);
		}

		return dataOutputView.getCopyOfBuffer();
	}

	public void migrateSerializedValue(
			DataInputDeserializer serializedOldValueInput,
			DataOutputSerializer serializedMigratedValueOutput,
			TypeSerializer<V> priorSerializer,
			TypeSerializer<V> newSerializer) throws StateMigrationException {

		try {
			V value = priorSerializer.deserialize(serializedOldValueInput);
			newSerializer.serialize(value, serializedMigratedValueOutput);
		} catch (Exception e) {
			throw new StateMigrationException("Error while trying to migrate RocksDB state.", e);
		}
	}

	byte[] getKeyBytes() {
		return serializeCurrentKeyWithGroupAndNamespace();
	}

	byte[] getValueBytes(V value) {
		try {
			dataOutputView.clear();
			valueSerializer.serialize(value, dataOutputView);
			return dataOutputView.getCopyOfBuffer();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing value", e);
		}
	}

	protected V getDefaultValue() {
		if (defaultValue != null) {
			return valueSerializer.copy(defaultValue);
		} else {
			return null;
		}
	}

	@Override
	public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		throw new UnsupportedOperationException("Global state entry iterator is unsupported for RocksDb backend");
	}

	public void updateItemFrequency(Object key){
		putStatsMetrics.updateKeyGroup(sharedKeyNamespaceSerializer.keyGroup, sharedKeyNamespaceSerializer.key);
		putStatsMetrics.updateGetItemFrequency(key);
		putStatsMetrics.updateCacheStat();
	}

	public void updateStateSize(long size){
		putStatsMetrics.updateStateSize(size);
	}

	public static class PutStatsMetrics<K> {
		final MetricGroup metricGroup;
		protected static final String STATE_NAME_KEY = "state_name";
		protected static final String ITEM_FREQUENCY = "itemFrequency";
		protected static final String STATE_SIZE = "stateSize";
		protected static final String CACHE_MISS = "cacheDataMiss";
		protected static final String CACHE_HIT = "cacheDataHit";

		private final ItemFrequencyGauge<K> itemFrequencyGauge = new ItemFrequencyGauge<K>();
		private final CacheStat cacheMiss;
		private final CacheStat cacheHit;
		private final AvgStateSizeGauge stateSizeGauge = new AvgStateSizeGauge();
		private int keyGroup;
		private K key;

		private long updateTime = 0;
		private static final long INTERVAL = 5000;

		//Todo: consider the namespace

		private PutStatsMetrics(MetricGroup metricGroup, String stateName, DBOptions dbOptions) {
			this.metricGroup =  metricGroup.addGroup(STATE_NAME_KEY, stateName);
			this.metricGroup.gauge(ITEM_FREQUENCY, itemFrequencyGauge);
			this.metricGroup.gauge(STATE_SIZE, stateSizeGauge);

			cacheHit = new CacheStat(dbOptions, "rocksdb.block.cache.data.hit");
			this.metricGroup.gauge(CACHE_HIT, cacheHit);

			cacheMiss = new CacheStat(dbOptions, "rocksdb.block.cache.data.miss");
			this.metricGroup.gauge(CACHE_MISS, cacheMiss);
		}

		private void updateKeyGroup(int keyGroup, K key){
			this.keyGroup = keyGroup;
			this.key = key;
		}

		private void updateGetItemFrequency(Object userKey){
			itemFrequencyGauge.putItem(key, userKey);
		}

		private void updateStateSize(final long size){
			stateSizeGauge.putStateSize(size);
		}

		private void updateCacheStat(){
			if(System.currentTimeMillis() - updateTime > INTERVAL) {
				cacheHit.update();
				cacheMiss.update();
				updateTime = System.currentTimeMillis();
			}
		}
	}

	public static class ItemFrequencyGauge<K> implements Gauge<Collection<Long>> {

		private final Map<K, Map<Object, AtomicLong>> itemFrequencyDummp = new HashMap<>();
		private final Map<Long, Long> outputs = new HashMap<>();

		public void putItem(K key, Object userKey) {
			if (userKey == null)
				userKey = key;
			Map<Object, AtomicLong> UserKeys = itemFrequencyDummp.get(key);
			AtomicLong count;
			if (UserKeys != null){
				count = UserKeys.get(userKey);
				if (count != null){
					count.addAndGet(1);
				} else {
					count = new AtomicLong(1);
					UserKeys.put(userKey, count);
				}
			} else {
				count = new AtomicLong(1);
				UserKeys = new HashMap<>();
				UserKeys.put(userKey, count);
				itemFrequencyDummp.put(key, UserKeys);
			}
			long counter = count.get();
			Long counterCount = outputs.get(counter);
			if(counterCount == null)
				counterCount = 1L;
			else counterCount += 1;
			outputs.put(counter, counterCount);

			counter = counter - 1;
			if(counter == 0)
				return;
			counterCount = outputs.get(counter);
			counterCount -= 1;
			outputs.put(counter, counterCount);
		}

		@Override
		public Collection<Long> getValue() {
			Collection<Long> results = new ArrayList<>();
			for (Map.Entry<Long, Long> entry : outputs.entrySet()){
				if(entry.getValue() > 0) {
					results.add(entry.getKey());
					results.add(entry.getValue());
				}
			}
			return results;
		}
	}

	public static class AvgStateSizeGauge implements Gauge<Tuple2<Double, Long>> {

		private double avgStateSize = 0;
		private long counter = 0;

		public void putStateSize(long stateSize) {
			double ratio = counter*1.0/(counter+1);
			avgStateSize = avgStateSize * ratio + stateSize * (1 - ratio);
			counter++;
		}

		@Override
		public Tuple2<Double, Long> getValue() {
			return new Tuple2<>(avgStateSize, counter);
		}
	}

	public static class CacheStat implements Gauge<Long>{

		private long stat;
		private DBOptions dbOptions;
		private String metrics;

		public CacheStat(DBOptions dbOptions, String metrics) {
			this.dbOptions = dbOptions;
			this.metrics = metrics;
		}

		public void update() {
				String[] lines = dbOptions.statistics().toString().split("\n");
				for (String line : lines){
					if (line.contains(metrics)){
						stat = Long.parseLong(line.split(" : ")[1]);
					}
				}
		}

		@Override
		public Long getValue() {
			return stat;
		}

	}
}
