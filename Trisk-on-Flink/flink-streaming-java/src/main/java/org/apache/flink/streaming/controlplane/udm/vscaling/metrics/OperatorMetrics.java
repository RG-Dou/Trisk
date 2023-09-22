package org.apache.flink.streaming.controlplane.udm.vscaling.metrics;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class OperatorMetrics {


	// Basic information
	private final String id;
	private final String operatorName;
	private final ArrayList<TaskMetrics> taskMetricsList = new ArrayList<>();
	private final LinearAlgorithm linearAlgorithm;
	private String stateName;
	private String stateTag;
	private boolean stateful = false;

	// Raw Data
	private double stateSize;
	// time reading from the mem
	private double alpha=0.0;
	// extra time reading from the disk compared to reading from the mem
	private double beta=0.0;
	private double k;

	public OperatorMetrics(String id, String name){
		this.id = id;
		this.operatorName = name;
		linearAlgorithm = new LinearAlgorithm(10);
	}

	public String getId() {
		return id;
	}

	public void addTask(TaskMetrics taskMetrics){
		taskMetricsList.add(taskMetrics);
	}

	public TaskMetrics getTaskMetrics(int index){
		return taskMetricsList.get(index);
	}

	public int getNumTasks(){
		return taskMetricsList.size();
	}

	public void addState(String stateName){
		this.stateName = stateName;
		stateful = true;
		for(TaskMetrics taskMetrics : taskMetricsList){
			StateMetrics state = new StateMetrics(stateName);
			taskMetrics.putState(stateName, state);
		}
	}

	public String getStateName(){
		if (stateful)
			return stateName;
		else return null;
	}

	public boolean isStateful(){
		return stateful;
	}

	public void setStateAccessTimeTag(String tag){
		stateTag = tag;
	}

	public String getStateTag(){
		return stateTag;
	}

	public String getOperatorName() {
		return operatorName;
	}

	public void updateStateTime(){
		for(TaskMetrics taskMetrics : taskMetricsList){
			taskMetrics.updateStateTime();
		}
	}

	public void updateStateSize(){
		long counter = 0;
		double sum = 0.0;
		for (TaskMetrics taskMetrics : taskMetricsList){
			StateMetrics state = taskMetrics.getStateMetric();
			counter += state.getAccessCounter();
			sum += state.getAccessCounter() * state.getStateSize();
		}
		stateSize = sum / counter;
	}

	public void updatek(){
		long recordsIn = 0;
		long stateAccessCounter = 0;
		for (TaskMetrics taskMetrics : taskMetricsList){
			recordsIn += taskMetrics.getRecordsIn();
			StateMetrics state = taskMetrics.getStateMetric();
			stateAccessCounter += state.getAccessCounter();
		}
		k = stateAccessCounter * 1.0 / recordsIn;
	}

	public void updateFrontEndTime(){
		for(TaskMetrics taskMetrics : taskMetricsList){
			taskMetrics.updateFrontEndTime(k);
		}
	}

	public void updateBacklog(){
		for(TaskMetrics taskMetrics : taskMetricsList){
			taskMetrics.updateBacklog();
		}
	}

	public void training(){
		for (TaskMetrics task : taskMetricsList){
			StateMetrics state = task.getStateMetric();
			linearAlgorithm.addData(1 - state.getHitRatio(), state.getAccessTime());
			System.out.println("Operator:" + operatorName + " TrainingData: missRatio:" + (1-state.getHitRatio()) + " stateTime:" + state.getAccessTime());
		}
		linearAlgorithm.excRegression();
		this.alpha = linearAlgorithm.getSlope();
		this.beta = linearAlgorithm.getIntercept();
	}

	public double getAlpha(){
		return alpha;
	}

	public double getBeta(){
		return beta;
	}

	public double getStateSize(){
		return stateSize;
	}

	public double getk(){
		return k;
	}

	public String taskInstancesToString(){
		StringBuilder builder = new StringBuilder();
		for (TaskMetrics task : taskMetricsList){
			builder.append(task.getInstanceID()).append(";");
		}
		builder.deleteCharAt(builder.lastIndexOf(";"));
		return builder.toString();
	}

	public String frontEndToString(){
		StringBuilder builder = new StringBuilder();
		for (TaskMetrics task : taskMetricsList){
			builder.append(task.getFrontEndTime()).append(";");
		}
		builder.deleteCharAt(builder.lastIndexOf(";"));
		return builder.toString();
	}

	public String backlogString(){
		StringBuilder builder = new StringBuilder();
		for (TaskMetrics task : taskMetricsList){
			builder.append(task.getBacklog()).append(";");
		}
		builder.deleteCharAt(builder.lastIndexOf(";"));
		return builder.toString();
	}

	public String arrivalRateString(){
		StringBuilder builder = new StringBuilder();
		for (TaskMetrics task : taskMetricsList){
			builder.append(task.getArrivalRate()).append(";");
		}
		builder.deleteCharAt(builder.lastIndexOf(";"));
		return builder.toString();
	}

	public String oldMemToString(){
		StringBuilder builder = new StringBuilder();
		for (TaskMetrics task : taskMetricsList){
			builder.append(task.getOptimalAllocation()).append(";");
		}
		builder.deleteCharAt(builder.lastIndexOf(";"));
		return builder.toString();
	}

	public String itemFrequencyToString(){
		StringBuilder builder = new StringBuilder();
		for (TaskMetrics task : taskMetricsList){
			builder.append(task.itemFrequencyToString()).append(";");
		}
		builder.deleteCharAt(builder.lastIndexOf(";"));
		return builder.toString();
	}

	public String cacheMissHistToString(int taskID){
		return taskMetricsList.get(taskID).cacheMissHistToString();
	}


	public String toSting(){
		String str = "operator: " + operatorName + ", id: " + id +
			", numTask: " + getNumTasks() +
			" {" + "\n";
		for (TaskMetrics task : taskMetricsList){
			str+=task.toString() + "\n";
		}
		return str+"}";
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
			regression.addData(0.0, 0);
		}
	}

	public double getSlope(){
		return regression.getSlope();
	}

	public double getIntercept(){
		return regression.getIntercept();
	}
}
