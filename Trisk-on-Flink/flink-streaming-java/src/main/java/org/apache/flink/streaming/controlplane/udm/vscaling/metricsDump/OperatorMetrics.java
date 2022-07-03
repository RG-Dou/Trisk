package org.apache.flink.streaming.controlplane.udm.vscaling.metricsDump;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class OperatorMetrics {


	private final String id;
	private final String operatorName;

	private final ArrayList<TaskMetrics> taskMetricsList = new ArrayList<>();
	private final ArrayList<String> stateNames = new ArrayList<>();
	private final Map<String, String> stateAccessTags = new HashMap<>();

	public OperatorMetrics(String id, String name){
		this.id = id;
		this.operatorName = name;
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
		if(stateNames.contains(stateName))
			return;
		stateNames.add(stateName);
		for(TaskMetrics taskMetrics : taskMetricsList){
			StateMetrics state = new StateMetrics(stateName);
			taskMetrics.putState(stateName, state);
		}
	}

	public ArrayList<String> getStateList(){
		return stateNames;
	}

	public int getNumStates(){
		return stateNames.size();
	}

	public void setStateAccessTimeTag(String stateName, String tag){
		stateAccessTags.put(stateName, tag);
		for(TaskMetrics taskMetrics : taskMetricsList){
			StateMetrics state = taskMetrics.getStateMetric(stateName);
			state.setAccessTimeTag(tag);
		}
	}

	public String getStateAccessTimeTag(String stateName){
		return this.stateAccessTags.get(stateName);
	}

	public String getOperatorName() {
		return operatorName;
	}

	public void updateStateTime(){
		for(TaskMetrics taskMetrics : taskMetricsList){
			taskMetrics.updateStateTime();
		}
	}

	public void updateTaskStateSize(){
		for(TaskMetrics taskMetrics : taskMetricsList){
			taskMetrics.updateStateSize();
		}
	}

	public void updateKandB(){
		for(TaskMetrics taskMetrics : taskMetricsList){
			taskMetrics.updateKandB();
		}
	}

	public void updateQueuingTime(){
		for(TaskMetrics taskMetrics : taskMetricsList){
			taskMetrics.updateQueuingTime();
		}
	}

	public void updateAlignmentTime(){
		for(TaskMetrics taskMetrics : taskMetricsList){
			taskMetrics.updateAlignmentTime();
		}
	}

	public void updateEndToEndLatency(){
		for(TaskMetrics taskMetrics : taskMetricsList){
			taskMetrics.updateEndToEndLatency();
		}
	}

	public String qtimeString(){
		StringBuilder builder = new StringBuilder();
		for (TaskMetrics task : taskMetricsList){
			builder.append(task.getQueuingTime()).append(";");
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

	public String ksString(){
		StringBuilder builder = new StringBuilder();
		for (TaskMetrics task : taskMetricsList){
			builder.append(task.ksString()).append(";");
		}
		builder.deleteCharAt(builder.lastIndexOf(";"));
		return builder.toString();
	}

	public String bsString(){
		StringBuilder builder = new StringBuilder();
		for (TaskMetrics task : taskMetricsList){
			builder.append(task.bsString()).append(";");
		}
		builder.deleteCharAt(builder.lastIndexOf(";"));
		return builder.toString();
	}

	public String stateSizeToString(){
		StringBuilder builder = new StringBuilder();
		for (TaskMetrics task : taskMetricsList){
			builder.append(task.getStateSize()).append(";");
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

	public String toSting(){
		String str = "operator: " + operatorName + ", id: " + id +
			", numTask: " + getNumTasks() + ", States: " + stateNames +
			" {" + "\n";
		for (TaskMetrics task : taskMetricsList){
			str+=task.toString() + "\n";
		}
		return str+"}";
	}
}
