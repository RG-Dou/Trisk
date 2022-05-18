package org.apache.flink.streaming.controlplane.udm.vscaling.metrics;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;
import java.util.Map;

public class TaskMetrics {


	private final int index;
	private final Map<String, StateMetrics> stateMetricsMap = new HashMap<>();

	private String slotID;

	private double queuingTime;
	private long recordsIn;
	private long recordsOut;
	private double tupleLatency;
	private double stateSize;
	private double optimalAllocation=0.0;

	private double alignmentTime;
	private double endToEndLatency;

	// Tuple latency statistics
	// f0. watermark -- the start counter of this time scheduling
	// f1. collection times
	// f2. the total tuple latency of this time scheduling
	private final Tuple3<Long, Long, Double> tupleLatencyStats = new Tuple3<>(0L, 0L, 0.0);
	private final Tuple3<Long, Long, Double> alignmentTimeStats = new Tuple3<>(0L, 0L, 0.0);
	private final Tuple3<Long, Long, Double> endToEndLatencyStats = new Tuple3<>(0L, 0L, 0.0);

	private final Tuple2<Long, Long> missStats = new Tuple2<>(0L, 0L);
	private final Tuple2<Long, Long> hitStats = new Tuple2<>(0L, 0L);

	private String rocksdbLogFileName = null;

	public TaskMetrics(int index){
		this.index = index;
	}

	public void putState(String name, StateMetrics metric){
		stateMetricsMap.put(name, metric);
	}

	public StateMetrics getStateMetric(String name){
		return stateMetricsMap.get(name);
	}

	public double getQueuingTime() {
		return queuingTime;
	}

	public void setQueuingTime(double queuingTime) {
		this.queuingTime = queuingTime;
	}

	public long getRecordsIn() {
		return recordsIn;
	}

	public long getRecordsOut() {
		return recordsOut;
	}

	public void setRecordsIn(long recordsIn) {
		this.recordsIn = recordsIn;
	}

	public void setRecordsOut(long recordsOut) {
		this.recordsOut = recordsOut;
	}

	public void setAlignmentTime(long latencyNano){
		setTuple3Stats(alignmentTimeStats, latencyNano * 1.0 / 1000000);
	}

	public void updateAlignmentTime(){
		alignmentTime = updateTuple3Stats(alignmentTimeStats);
	}

	public double getAlignmentTime(){
		return alignmentTime;
	}

	public void setEndToEndLatency(double avgLatency){
		setTuple3Stats(endToEndLatencyStats, avgLatency);
	}

	public void updateEndToEndLatency(){
		endToEndLatency = updateTuple3Stats(endToEndLatencyStats);
	}

	public double getEndToEndLatency(){
		return endToEndLatency;
	}

	public double getTupleLatency() {
		return tupleLatency;
	}

	public void setTupleLatency(double tupleLatency) {
		setTuple3Stats(tupleLatencyStats, tupleLatency);
	}

	// Calculate the average tuple latency
	public void updateTupleLatency(){
		tupleLatency = updateTuple3Stats(tupleLatencyStats);
	}

	public void setTuple3Stats(Tuple3<Long, Long, Double> stats, double value){
		stats.f2 += value;
		stats.f1 += 1;
	}

	public double updateTuple3Stats(Tuple3<Long, Long, Double> stats){
		double avgValue = stats.f2/(stats.f1 - stats.f0);
		stats.f0 = stats.f1;
		stats.f2 = 0.0;
		return avgValue;
	}

	public double getStateSize() {
		return stateSize;
	}

	public void updateStateTime(){
		for (Map.Entry<String, StateMetrics> entry : stateMetricsMap.entrySet()){
			StateMetrics stateMetric = entry.getValue();
			stateMetric.updateStateTime(recordsIn);
		}
	}

	public void updateStateSize() {
		stateSize = 0.0;
		long totalCounter = 0;
		for (Map.Entry<String, StateMetrics> entry : stateMetricsMap.entrySet()){
			StateMetrics stateMetric = entry.getValue();
			totalCounter += stateMetric.getAccessCounter();
		}
		for (Map.Entry<String, StateMetrics> entry : stateMetricsMap.entrySet()){
			StateMetrics stateMetric = entry.getValue();
			stateSize += stateMetric.getAccessCounter() * 1.0 / totalCounter * stateMetric.getStateSize();
		}
	}

	public void updateKandB(){
		for (Map.Entry<String, StateMetrics> entry : stateMetricsMap.entrySet()){
			StateMetrics stateMetric = entry.getValue();
			stateMetric.updateKandB();
		}
	}

	public void updateQueuingTime(){
		// before update queueing time, we should update tuple latency first
		updateTupleLatency();

		queuingTime = tupleLatency;
		for (Map.Entry<String, StateMetrics> entry : stateMetricsMap.entrySet()){
			StateMetrics stateMetric = entry.getValue();
			queuingTime -= stateMetric.getAccessTime();
		}
	}

	public double getOptimalAllocation() {
		return optimalAllocation;
	}

	public void setOptimalAllocation(double optimalAllocation) {
		this.optimalAllocation = optimalAllocation;
	}

	public void setCacheHitMiss(long hit, long miss){
		hitStats.f0 = hitStats.f1;
		hitStats.f1 = hit;
		long deltaHit = hitStats.f1 - hitStats.f0;

		missStats.f0 = missStats.f1;
		missStats.f1 = miss;
		long deltaMiss = missStats.f1 - missStats.f0;

		double hitRatio = 0.0;
		if(deltaHit + deltaMiss > 0){
			hitRatio = deltaHit * 1.0 / (deltaHit + deltaMiss);
		}

		//hit ratio
		for(Map.Entry<String, StateMetrics> entry : stateMetricsMap.entrySet()){
			entry.getValue().setHitRatio(hitRatio);
		}
	}

	public int getIndex() {
		return index;
	}

	public String ksString(){
		StringBuilder builder = new StringBuilder();
		for (Map.Entry<String, StateMetrics> entry : stateMetricsMap.entrySet()){
			builder.append(entry.getValue().getK()).append(",");
		}
		builder.deleteCharAt(builder.lastIndexOf(","));
		return builder.toString();
	}

	public String bsString(){
		StringBuilder builder = new StringBuilder();
		for (Map.Entry<String, StateMetrics> entry : stateMetricsMap.entrySet()){
			builder.append(entry.getValue().getB()).append(",");
		}
		builder.deleteCharAt(builder.lastIndexOf(","));
		return builder.toString();
	}

	public String itemFrequencyToString(){
		StringBuilder builder = new StringBuilder();
		for (Map.Entry<String, StateMetrics> entry : stateMetricsMap.entrySet()){
			builder.append(entry.getValue().itemFrequencyToString()).append(",");
		}
		builder.deleteCharAt(builder.lastIndexOf(","));
		return builder.toString();
	}

	public String getSlotID() {
		return slotID;
	}

	public void setSlotID(String slotID) {
		this.slotID = slotID;
	}

	public String getRocksdbLogFileName() {
		return rocksdbLogFileName;
	}

	public void setRocksdbLogFileName(String rocksdbLogFileName) {
		this.rocksdbLogFileName = rocksdbLogFileName;
	}

	public String toString(){
		String str = "  task: " + index +
			", optimal allocation: " + getOptimalAllocation() +", tuple latency: " + getTupleLatency() + ", queuing Time:" + getQueuingTime() +", recordsIn: " + getRecordsIn() + ", stateSize: " + getStateSize()+
			", { \n";
		for (Map.Entry<String, StateMetrics> entry : stateMetricsMap.entrySet()){
			str+=entry.getValue().toString() + "\n";
		}
		return str+"  }";
	}

}
