package org.apache.flink.streaming.controlplane.udm.vscaling.metrics;

import java.util.*;

public class VScalingMetrics {

	//operatorFullList, operatorFullMap: all operators of this job.
	private final ArrayList<String> operatorFullList = new ArrayList<>();
	private final Map<String, OperatorMetrics> operatorFullMap = new HashMap<>();

	//operatorList, operatorMap: all stateful operators of this job.
	private final ArrayList<String> operatorList = new ArrayList<>();
	private final Map<String, OperatorMetrics> operatorMap = new HashMap<>();

	private final long totalMem;
	private long epoch = 0;

	// Algorithm Output
	private String algorithmInfo = "false";
	private String cheApproxInfo;

	// Slot information
	private final Map<String, SlotMetrics> slotsMap = new HashMap<>();

	// Shrinking and expansion slots
	private final List<SlotMetrics> shrinkingSlots = new ArrayList<>();
	private final List<SlotMetrics> expansionSlots = new ArrayList<>();

	public VScalingMetrics(long totalMem){
		this.totalMem = totalMem;
		epoch = 1;
	}

	public int getNumOperator() {
		return operatorList.size();
	}

	public int getFullNumOperator(){
		return operatorFullList.size();
	}

	public void addOperator(String id, String name) {
		OperatorMetrics operatorMetrics = new OperatorMetrics(id, name);
		this.operatorFullList.add(id);
		this.operatorFullMap.put(id, operatorMetrics);
	}

	public ArrayList<String> getOperatorList(){
		return operatorList;
	}

	public ArrayList<String> getOperatorFullList(){
		return operatorFullList;
	}

	public OperatorMetrics getOperator(String id){
		return operatorFullMap.get(id);
	}

	public void setNumTask(String operatorId, int numTasks){
		OperatorMetrics operator = operatorFullMap.get(operatorId);
		for (int i = 0; i < numTasks; i ++){
			TaskMetrics task = new TaskMetrics(i);
			operator.addTask(task);
		}
	}

	public Integer getNumTask(String operatorId){
		return operatorMap.get(operatorId).getNumTasks();
	}

	public String numTasksToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).getNumTasks()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public void setStateName(String operatorId, String stateName){
		if(!operatorMap.containsKey(operatorId)){
			OperatorMetrics operator = operatorFullMap.get(operatorId);
			operatorList.add(operatorId);
			operatorMap.put(operatorId, operator);
		}
		operatorMap.get(operatorId).addState(stateName);
	}

	public ArrayList<String> getOperatorState(String operatorId){
		return operatorMap.get(operatorId).getStateList();
	}

	public String numStatesToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).getNumStates()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public void setStateAccessTimeTag(String operatorId, String stateName, String tag){
		operatorMap.get(operatorId).setStateAccessTimeTag(stateName, tag);
	}

	public String getStateAccessTimeTag(String operatorId, String stateName){
		return operatorMap.get(operatorId).getStateAccessTimeTag(stateName);
	}

	public long getTotalMem() {
		return totalMem;
	}

	public String qTimeToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).qtimeString()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public void setAccessTime(String operatorId, int taskIndex, String stateName, double accessTime){
		operatorMap.get(operatorId).getTaskMetrics(taskIndex).getStateMetric(stateName).setAccessTime(accessTime);
	}

	public void setTupleLatency(String operatorId, int taskIndex, double latency){
		operatorFullMap.get(operatorId).getTaskMetrics(taskIndex).setTupleLatency(latency);
	}

	public void setNumRecordsIn(String operatorId, int taskIndex, long recordsIn){
		operatorFullMap.get(operatorId).getTaskMetrics(taskIndex).setRecordsIn(recordsIn);
	}

	public void setNumRecordsOut(String operatorId, int taskIndex, long recordsOut){
		operatorFullMap.get(operatorId).getTaskMetrics(taskIndex).setRecordsOut(recordsOut);
	}

	public void setAlignmentTime(String operatorId, int taskIndex, long latencyNano){
		operatorFullMap.get(operatorId).getTaskMetrics(taskIndex).setAlignmentTime(latencyNano);
	}

	public void setEndToEndLatency(String operatorId, int taskIndex, double avgLatency){
		operatorFullMap.get(operatorId).getTaskMetrics(taskIndex).setEndToEndLatency(avgLatency);
	}

	public String ksToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).ksString()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public String bsToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).bsString()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public void setStateSizesState(String operatorId, int taskIndex, String stateName, double size){
		operatorMap.get(operatorId).getTaskMetrics(taskIndex).getStateMetric(stateName).setStateSize(size);
	}

	public void setStateSizesCounter(String operatorId, int taskIndex, String stateName, long counter){
		operatorMap.get(operatorId).getTaskMetrics(taskIndex).getStateMetric(stateName).setAccessCounter(counter);
	}

	// Preprocess information, such as state access time, state size, queueing time
	public void preprocess(){
		updateStateTime();
		updateStateSizesTask();
		updateQueuingTime();
		updateKandB();
		updateAlignmentTime();
		updateEndToEndLatency();
	}

	private void updateStateTime(){
		for(String operatorId : operatorList){
			operatorMap.get(operatorId).updateStateTime();
		}
	}

	private void updateStateSizesTask(){
		for(String operatorId : operatorList){
			operatorMap.get(operatorId).updateTaskStateSize();
		}
	}

	public void updateKandB(){
		for(String operatorId : operatorList){
			operatorMap.get(operatorId).updateKandB();
		}
	}

	private void updateQueuingTime(){
		for(String operatorId : operatorList){
			operatorMap.get(operatorId).updateQueuingTime();
		}
	}

	private void updateAlignmentTime(){
		for(String operatorId : operatorFullList){
			operatorFullMap.get(operatorId).updateAlignmentTime();
		}
	}

	private void updateEndToEndLatency(){
		for(String operatorId : operatorFullList){
			operatorFullMap.get(operatorId).updateEndToEndLatency();
		}
	}

	public String stateSizesTaskToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).stateSizeToString()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public void setItemFrequency(String operatorId, int taskIndex, String stateName, ArrayList<Long> frequency){
		operatorMap.get(operatorId).getTaskMetrics(taskIndex).getStateMetric(stateName).setItemFrequency(frequency);
	}

	public String itemFrequencyToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).itemFrequencyToString()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public String oldMemToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).oldMemToString()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public long getEpoch() {
		return epoch;
	}

	public void incEpoch(){
		epoch++;
	}

	public void setAlgorithmInfo(String info){
		this.algorithmInfo = info;
	}

	public String getAlgorithmInfo(){
		return algorithmInfo;
	}

	public String getCheApproxInfo() {
		return cheApproxInfo;
	}

	public void setCheApproxInfo(String cheApproxInfo) {
		this.cheApproxInfo = cheApproxInfo;
	}

	public void setOptimalAllocation(String operatorId, int taskIndex, double size){
		operatorMap.get(operatorId).getTaskMetrics(taskIndex).setOptimalAllocation(size);
	}

	public void addSlotInfo(String slotID, SlotMetrics slot){
		slotsMap.put(slotID, slot);
	}

	public SlotMetrics getSlot(String slotID){
		return slotsMap.get(slotID);
	}
	public Collection<String> getSlotsList(){
		return slotsMap.keySet();
	}

	public void addShrink(SlotMetrics slot){
		shrinkingSlots.add(slot);
	}
	public List<SlotMetrics> getShrink(){
		return shrinkingSlots;
	}
	public void addExpand(SlotMetrics slot){
		expansionSlots.add(slot);
	}
	public List<SlotMetrics> getExpand(){
		return expansionSlots;
	}

	public void clearGroup(){
		this.expansionSlots.clear();
		this.shrinkingSlots.clear();
	}

}
