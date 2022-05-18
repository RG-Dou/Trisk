package org.apache.flink.streaming.controlplane.udm.vscaling.metrics;

import org.apache.flink.runtime.resourcemanager.slotmanager.TaskManagerSlot;

import java.util.ArrayList;
import java.util.List;

public class SlotMetrics {
	private final String id;
	private final TaskManagerSlot slot;
	private final List<TaskMetrics> tasks = new ArrayList<>();

	private String type;

	private Long oldMemSize;
	private Long targetMemSize;

	public SlotMetrics(String id, TaskManagerSlot slot){
		this.id = id;
		this.slot = slot;
		type = "beyond this job";
	}

	public String getId(){
		return id;
	}

	public TaskManagerSlot getSlot(){
		return slot;
	}

	public void addTask(TaskMetrics task){
		tasks.add(task);
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Long getOldMemSize() {
		return oldMemSize;
	}

	public void setOldMemSize(Long oldMemSize) {
		this.oldMemSize = oldMemSize;
	}

	public Long getTargetMemSize() {
		return targetMemSize;
	}

	public void setTargetMemSize(Long targetMemSize) {
		this.targetMemSize = targetMemSize;
	}


}
