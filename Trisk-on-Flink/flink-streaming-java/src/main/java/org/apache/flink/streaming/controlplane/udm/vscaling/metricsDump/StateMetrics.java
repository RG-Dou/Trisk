package org.apache.flink.streaming.controlplane.udm.vscaling.metricsDump;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.commons.math3.stat.regression.SimpleRegression;
// RandomDataGererator has zipfdistribution

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class StateMetrics {

	private final String name;
	private final LinearAlgorithm linearAlgorithm;
	private String accessTimeTag;

	private double k;
	private double b;
	private double stateSize;
	private double accessTime;
	private long accessCounter;
	private ArrayList<Long> itemFrequency;
	private double hitRatio;

	// f0. watermark -- the start counter of this time scheduling
	// f1. collection times
	// f2. the total access time of this time scheduling
	private final Tuple3<Long, Long, Double> stateTimeStats = new Tuple3<>(0L, 0L, 0.0);

	public StateMetrics(String name){
		this.name = name;
		linearAlgorithm = new LinearAlgorithm(10);
	}

	public void setAccessTimeTag(String accessTimeTag) {
		this.accessTimeTag = accessTimeTag;
	}

	public double getK() {
		return k;
	}

	public double getB() {
		return b;
	}

	//ToDo: how to update K and B
	public void updateKandB(){
		linearAlgorithm.addData(hitRatio, accessTime);
		linearAlgorithm.excRegression();
		this.k = linearAlgorithm.getSlope();
		this.b = linearAlgorithm.getIntercept();
	}

	public double getStateSize() {
		return stateSize;
	}

	public void setStateSize(double stateSize) {
		this.stateSize = stateSize / 1024 / 1024;
	}

	public double getAccessTime() {
		return accessTime;
	}

	public void setAccessTime(double accessTime) {
		stateTimeStats.f2 += accessTime;
		stateTimeStats.f1 += 1;
	}

	public long getAccessCounter() {
		return accessCounter;
	}

	public void setAccessCounter(long accessCounter) {
		this.accessCounter = accessCounter;
	}

	public void updateStateTime(long recordsIn){
		// Calculate the average time of one access
		accessTime = stateTimeStats.f2 / (stateTimeStats.f1 - stateTimeStats.f0);

//		System.out.println("Average time of one access: " + accessTime + ", average counter: " + (this.accessCounter * 1.0 / recordsIn));
		// times the average access times.
		accessTime = this.accessCounter * 1.0 / recordsIn * accessTime;

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
		return "    state: " + name + ", " + accessTimeTag +
			", k:" + getK() + ", b:" + getB() + ", access time: " + getAccessTime() + ", stateSize: " + getStateSize() + ", accessCounter: " + getAccessCounter() + ", itemFrequency: " + getItemFrequency() + "\n";
	}
}

class LinearAlgorithm{

	private final int totalRegions;
	private final SimpleRegression regression = new SimpleRegression();

	//hit ratio
	private final Map<Integer, Tuple2<Long, Double>> xs = new HashMap<>();
	//access time
	private final Map<Integer, Tuple2<Long, Double>> ys = new HashMap<>();

	public LinearAlgorithm(int totalRegions){
		this.totalRegions = totalRegions;
	}

	public void addData(double x, double y){
		int region = (int) (x * 100) / totalRegions;
		Tuple2<Long, Double> xRecords = xs.get(region);
		if(xRecords == null)
			xRecords = new Tuple2<>(0L, 0.0);
		xRecords.f1 += x;
		xRecords.f0 += 1;
		xs.put(region, xRecords);

		Tuple2<Long, Double> yRecords = ys.get(region);
		if(yRecords == null)
			yRecords = new Tuple2<>(0L, 0.0);
		yRecords.f1 += y;
		yRecords.f0 += 1;
		ys.put(region, yRecords);
	}

	public void excRegression(){
		regression.clear();
		for(Map.Entry<Integer, Tuple2<Long, Double>> entry : xs.entrySet()){
			int region = entry.getKey();
			Tuple2<Long, Double> xRecords = entry.getValue();
			Tuple2<Long, Double> yRecords = ys.get(region);
			regression.addData(xRecords.f1 / xRecords.f0, yRecords.f1 / yRecords.f0);
		}
		if(xs.size() <= 1){
			regression.addData(1.0, 0);
		}
	}

	public double getSlope(){
		return regression.getSlope();
	}

	public double getIntercept(){
		return regression.getIntercept();
	}
}
