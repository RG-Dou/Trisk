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
	private double decayRate = 0.8;

	// Slot information
	private final Map<String, SlotMetrics> slotsMap = new HashMap<>();
	private final Map<Integer, List<SlotMetrics>> slotTMMap = new HashMap<>();

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

	public String taskInstancesToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).taskInstancesToString()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public String cacheMissHistToString(String operatorID,int taskID){
		return operatorMap.get(operatorID).cacheMissHistToString(taskID);
	}

	public void setStateName(String operatorId, String stateName){
		if(!operatorMap.containsKey(operatorId)){
			OperatorMetrics operator = operatorFullMap.get(operatorId);
			operatorList.add(operatorId);
			operatorMap.put(operatorId, operator);
		}
		operatorMap.get(operatorId).addState(stateName);
	}

	public String getOperatorState(String operatorId){
		return operatorMap.get(operatorId).getStateName();
	}

	public void setStateAccessTimeTag(String operatorId, String tag){
		operatorMap.get(operatorId).setStateAccessTimeTag(tag);
	}

	public String getStateAccessTimeTag(String operatorId){
		return operatorMap.get(operatorId).getStateTag();
	}

	public long getTotalMem() {
		return totalMem;
	}

	public void setAccessTime(String operatorId, int taskIndex, double accessTime){
		operatorMap.get(operatorId).getTaskMetrics(taskIndex).getStateMetric().setAccessTime(accessTime);
	}

	public void setQueuingDelay(String operatorId, int taskIndex, double queuingTime){
		operatorFullMap.get(operatorId).getTaskMetrics(taskIndex).setQueuingTime(queuingTime, decayRate);
	}

	public void setServiceTime(String operatorId, int taskIndex, double serviceTime){
		operatorFullMap.get(operatorId).getTaskMetrics(taskIndex).setServiceTime(serviceTime, decayRate);
	}

	public void setNumRecordsIn(String operatorId, int taskIndex, long recordsIn){
		operatorFullMap.get(operatorId).getTaskMetrics(taskIndex).setRecordsIn(recordsIn);
	}

	public void setArrivalRate(String operatorId, int taskIndex, double rate){
		operatorFullMap.get(operatorId).getTaskMetrics(taskIndex).setArrivalRate(rate);
	}

	public void setCacheHitMiss(String operatorId, int taskIndex, long hit, long miss){
		operatorFullMap.get(operatorId).getTaskMetrics(taskIndex).setCacheHitMiss(hit, miss);
	}

	public void setAlignmentTime(String operatorId, int taskIndex, long latencyNano){
		operatorFullMap.get(operatorId).getTaskMetrics(taskIndex).setAlignmentTime(latencyNano);
	}

	public String frontEndToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).frontEndToString()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public String kToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).getk()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public String alphaToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).getAlpha()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public String betaToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).getBeta()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public String backlogToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).backlogString()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public String arrivalRateToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).arrivalRateString()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public void setStateSizesState(String operatorId, int taskIndex, double size){
		operatorMap.get(operatorId).getTaskMetrics(taskIndex).getStateMetric().setStateSize(size);
	}

	public void setStateSizesCounter(String operatorId, int taskIndex, String stateName, long counter){
		operatorMap.get(operatorId).getTaskMetrics(taskIndex).getStateMetric().setAccessCounter(counter);
	}

	// Preprocess information, such as state access time, state size, queueing time
	public void preprocess(){
		updateStateSizesTask();
		updateStateTime();
		updatek();
		updateFrontEndTime();
		updateBacklog();
		training();
	}

	private void updateStateSizesTask(){
		for(String operatorId : operatorList){
			operatorMap.get(operatorId).updateStateSize();
		}
	}

	private void updateStateTime(){
		for(String operatorId : operatorList){
			operatorMap.get(operatorId).updateStateTime();
		}
	}

	private void updatek(){
		for(String operatorId : operatorList){
			operatorMap.get(operatorId).updatek();
		}
	}

	private void updateFrontEndTime(){
		for(String operatorId : operatorList){
			operatorMap.get(operatorId).updateFrontEndTime();
		}
	}

	private void updateBacklog(){
		for(String operatorId : operatorList){
			operatorMap.get(operatorId).updateBacklog();
		}
	}

	public void training(){
		for(String operatorId : operatorList){
			operatorMap.get(operatorId).training();
		}
	}

	public String stateSizesTaskToString(){
		StringBuilder result = new StringBuilder();
		for(String operatorId : operatorList){
			result.append(operatorMap.get(operatorId).getStateSize()).append("|");
		}
		result.deleteCharAt(result.lastIndexOf("|"));
		return result.toString();
	}

	public void setItemFrequency(String operatorId, int taskIndex, String stateName, ArrayList<Long> frequency){
		operatorMap.get(operatorId).getTaskMetrics(taskIndex).getStateMetric().setItemFrequency(frequency);
	}

	public void setStateHist(String operatorId, int taskIndex, long cacheSize){
		operatorMap.get(operatorId).getTaskMetrics(taskIndex).getStateMetric().appendHist(cacheSize);
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

	public void addSlotTMInfo(int instanceID, SlotMetrics slot){
		List<SlotMetrics> slots = slotTMMap.get(instanceID);
		if(slots == null){
			slots = new ArrayList<>();
		}
		if(!slots.contains(slot)) {
			slots.add(slot);
			slotTMMap.put(instanceID, slots);
		}
	}

	public Map<Integer, List<SlotMetrics>> getSlotTMMap(){
		return slotTMMap;
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

	public void setDecayRate(double decayRate){
		this.decayRate = decayRate;
	}
}
