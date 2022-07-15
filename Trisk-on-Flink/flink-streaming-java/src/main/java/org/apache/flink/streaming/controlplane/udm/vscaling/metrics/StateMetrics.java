package org.apache.flink.streaming.controlplane.udm.vscaling.metrics;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

public class StateMetrics {

	private final String name;

	private double accessTime;
	private long accessCounter;
	private double stateSize;
	private ArrayList<Long> itemFrequency;
	private double hitRatio;
	private double stateTimeInstant;

	// f0. watermark -- the start counter of this time scheduling
	// f1. collection times
	// f2. the total access time of this time scheduling
	private final Tuple3<Long, Long, Double> stateTimeStats = new Tuple3<>(0L, 0L, 0.0);

	public StateMetrics(String name){
		this.name = name;
	}

	public double getAccessTime() {
		return accessTime;
	}

	public double getAccessTimeInstant() {
		return stateTimeInstant;
	}

	public void setAccessTime(double accessTime) {
		stateTimeInstant = accessTime;
		stateTimeStats.f2 += accessTime;
		stateTimeStats.f1 += 1;
	}

	public long getAccessCounter() {
		return accessCounter;
	}

	public void setAccessCounter(long accessCounter) {
		this.accessCounter = accessCounter;
	}

	public void setStateSize(double stateSize){
		this.stateSize = stateSize / 1024 / 1024;
	}

	public double getStateSize(){
		return stateSize;
	}

	public long getTotalItems(){
		long num = 0;
		for (int i = 1; i < itemFrequency.size(); i = i + 2){
			num += itemFrequency.get(i);
		}
		return num;
	}

	public void updateStateTime(long recordsIn){
		// Calculate the average time of one access
		accessTime = stateTimeStats.f2 / (stateTimeStats.f1 - stateTimeStats.f0);

		stateTimeStats.f0 = stateTimeStats.f1;
		stateTimeStats.f2 = 0.0;
	}

	public ArrayList<Long> getItemFrequency() {
		return itemFrequency;
	}

	public void setItemFrequency(ArrayList<Long> itemFrequency) {
		this.itemFrequency = itemFrequency;
	}

	public double getHitRatio() {
		return hitRatio;
	}

	public void setHitRatio(double hitRatio) {
		this.hitRatio = hitRatio;
	}


	public String itemFrequencyToString(){
		StringBuilder builder = new StringBuilder();
		for (Long item : itemFrequency){
			builder.append(item).append("-");
		}
		builder.deleteCharAt(builder.lastIndexOf("-"));
		return builder.toString();
	}

	public String getName() {
		return name;
	}

	public String toString(){
		return "    state: " + name + ", " +
			", access time: " + getAccessTime() + ", stateSize: " + ", accessCounter: " + getAccessCounter() + ", itemFrequency: " + getItemFrequency() + "\n";
	}
}
