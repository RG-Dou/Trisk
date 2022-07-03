package org.apache.flink.streaming.controlplane.udm.vscaling.metrics;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;
import java.util.Map;

public class TaskMetrics {


	// Basic Information
	private final int index;
	private String stateName;
	private StateMetrics stateMetric;
	private String slotID;
	private int InstanceID;

	//Raw Data
	private double serviceTime;
	private double queuingTime;
	private long recordsIn;
	private double alignmentTime;
	private final Tuple2<Long, Long> missStats = new Tuple2<>(0L, 0L);
	private final Tuple2<Long, Long> hitStats = new Tuple2<>(0L, 0L);

	// Mature Data
//	private double frontEndTime;
	private final Tuple2<Double, Long> frontEndTime = new Tuple2<>(0.0, 0L);
	private long backlog;

	private double optimalAllocation=0.0;


	private String rocksdbLogFileName = null;

	public TaskMetrics(int index){
		this.index = index;
	}

	public void putState(String name, StateMetrics metric){
		stateName = name;
		stateMetric = metric;
	}

	public StateMetrics getStateMetric(){
		return stateMetric;
	}

	public String getStateName() {
		return stateName;
	}

	public void setQueuingTime(double queuingTime) {
		this.queuingTime = queuingTime;
	}

	public double getQueuingTime() {
		return queuingTime;
	}

	public void setServiceTime(double serviceTime){
		this.serviceTime = serviceTime;
	}

	public double getServiceTime(){
		return serviceTime;
	}

	public void setRecordsIn(long recordsIn) {
		this.recordsIn = recordsIn;
	}

	public long getRecordsIn() {
		return recordsIn;
	}

	public void setAlignmentTime(double time) {
		this.alignmentTime = time;
	}

	public double getAlignmentTime(){
		return alignmentTime;
	}

	public void updateFrontEndTime(double k){
		frontEndTime.f0 += serviceTime - stateMetric.getAccessTime() * k;
		frontEndTime.f1 += 1;
	}

	public double getFrontEndTime(){
		return frontEndTime.f0 / frontEndTime.f1;
	}

	public void updateBacklog(){
		backlog = (long) (queuingTime / serviceTime);
	}

	public long getBacklog() {
		return backlog;
	}

	public void updateStateTime(){
		stateMetric.updateStateTime(recordsIn);
	}

	public void setOptimalAllocation(double optimalAllocation) {
		this.optimalAllocation = optimalAllocation;
	}

	public double getOptimalAllocation(){
		return optimalAllocation;
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
		stateMetric.setHitRatio(hitRatio);
	}

	public int getIndex() {
		return index;
	}

	public String itemFrequencyToString(){
		return stateMetric.itemFrequencyToString();
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

	public int getInstanceID() {
		return InstanceID;
	}

	public void setInstanceID(int instanceID) {
		InstanceID = instanceID;
	}


	public String toString(){
		String str = "  task: " + index +
			", optimal allocation: " + getOptimalAllocation() + ", queuing Time:" + getQueuingTime() +", recordsIn: " + getRecordsIn() +
			", { \n";
		str+=stateMetric.toString() + "\n";
		return str+"  }";
	}

}
