package org.apache.flink.streaming.controlplane.udm.vscaling;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.slotmanager.TaskManagerSlot;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.TriskWithLock;
import org.apache.flink.streaming.controlplane.udm.vscaling.algorithm.*;
import org.apache.flink.streaming.controlplane.udm.vscaling.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

// ToDo: comments and logs
public class ElasticMemoryManager extends AbstractMemoryManager {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticMemoryManager.class);
	private final TestingThread testingThread;
	private final Algorithm algorithm;

	private final String SCHEDULE_INTERVAL = "trisk.config.schedule_interval";
	private final String AlgorithmPath = "trisk.vScaling.python.path";
	private final String AlgorithmType = "trisk.vScaling.python.type";
	private final Long scheduleInterval;
	private boolean onScheduling = false;
	private long scheduleTime;

	public ElasticMemoryManager(ReconfigurationExecutor reconfigurationExecutor, Configuration configuration) {
		super(reconfigurationExecutor, configuration);
		scheduleInterval = configuration.getLong(SCHEDULE_INTERVAL, 20000);
		// ToDo: the python path should be a relative path: "Trisk-on-Flink/flink-tools/"
		String algorithmType = configuration.getString(AlgorithmType, "CacheMissEqn");
		String algorithmPath = configuration.getString(AlgorithmPath, "/home/drg/projects/work3/flink/alg-data/");
		if (algorithmType.equals("Che"))
			algorithm = new PythonAlgorithm(metrics, algorithmPath);
		else
			algorithm = new CacheMissEqnAlgorithm(metrics, algorithmPath);
		testingThread = new TestingThread();
	}

	@Override
	public synchronized void startControllers() {
		System.out.println("Testing TestingController is starting...");
		testingThread.setName("reconfiguration test");
		testingThread.start();
	}

	@Override
	public void stopControllers() {
		System.out.println("Testing TestingController is stopping...");
		showOperatorInfo();
	}

	@Override
	public void onChangeStarted() throws InterruptedException {
		// wait for operation completed
		synchronized (object) {
			object.wait();
		}
	}

	@Override
	public synchronized void onChangeCompleted(Throwable throwable) {
		if(throwable != null){
			testingThread.interrupt();
		}
		System.out.println("my self defined instruction finished??");
		synchronized (object) {
			object.notify();
		}
	}

	@Override
	public boolean updateSlotInfo(){
		TriskWithLock planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
		Collection<TaskManagerSlot> allSlots = getReconfigurationExecutor().getAllSlots();
		// Create slot information for all slots, include other jobs' slots, this job's slots: stateful and stateless slots.
		Map<String, SlotMetrics> allSlotsMetrics = new HashMap<>();
		ArrayList<String> taskManagers = new ArrayList<>();
		for(TaskManagerSlot slot: allSlots){
			String slotID = slot.getSlotId().toString();
			String instanceID = slot.getInstanceId().toString();
			if (!taskManagers.contains(instanceID))
				taskManagers.add(instanceID);
			SlotMetrics slotMetric = new SlotMetrics(slotID, slot, taskManagers.indexOf(instanceID));
			slotMetric.setOldMemSize(slot.getResourceProfile().getManagedMemory().getBytes() / 1024 / 1024);
			allSlotsMetrics.put(slotID, slotMetric);
		}

		for(String operatorId : metrics.getOperatorFullList()){
			OperatorMetrics operator = metrics.getOperator(operatorId);
			int numTasks = operator.getNumTasks();
			String operatorName = operator.getOperatorName();
			int testOpID = findOperatorByName(operatorName);
			boolean isStateful = (metrics.getOperatorList().contains(operatorId));
			for(int i = 0; i < numTasks; i++){
				TaskMetrics taskMetric = operator.getTaskMetrics(i);
				String slotID = planWithLock.getExecutionPlan().getTaskResource(testOpID, i).resourceSlot;
				// Set slot for task.
				operator.getTaskMetrics(i).setSlotID(slotID);

				// Set type for this slot.
				SlotMetrics slotMetric = allSlotsMetrics.get(slotID);
				int instanceID = slotMetric.getTaskInstance();
				metrics.addSlotTMInfo(instanceID, slotMetric);
				taskMetric.setInstanceID(instanceID);
				if(isStateful && slotMetric.getType().equals("stateful")) {

					// One slot has two stateful task; we do not allow this.
					LOG.info("error configuration: one slot has more than one stateful task!!");
					return false;
				} else if(isStateful) {

					// if the task is stateful and the slot does not have stateful task before.
					slotMetric.setType("stateful");
				} else if(!slotMetric.getType().equals("stateful")) {

					// if the task is stateless, and the slot has no stateful task yet.
					slotMetric.setType("stateless");
				}

				taskMetric.setOptimalAllocation(slotMetric.getOldMemSize());

				// Add task to the slot metric
				slotMetric.addTask(taskMetric);
				metrics.addSlotInfo(slotID, slotMetric);
			}
		}
		return true;
	}

	private void startResize(){
		grouping();
		if(metrics.getShrink().size() == 0 && metrics.getExpand().size() == 0){
			onScheduling = false;
			scheduleTime = System.currentTimeMillis();
			return;
		}

		LOG.info("Start Resize Memory.");
		resizeGroup(new ArrayList<>(metrics.getShrink()));
		resizeGroup(new ArrayList<>(metrics.getExpand()));
	}

	private void grouping(){

		// clear shrink and expand;
//		metrics.clearGroup();

		// set for stateful slot.
		Collection<String> slots = new ArrayList<>(metrics.getSlotsList());
		for (String operatorID : metrics.getOperatorList()){
			int numTasks = metrics.getOperator(operatorID).getNumTasks();
			for(int i = 0; i < numTasks; i++){
				TaskMetrics taskMetrics = metrics.getOperator(operatorID).getTaskMetrics(i);
				String slotID = taskMetrics.getSlotID();
				SlotMetrics slotMetrics = metrics.getSlot(slotID);
				long target = (long)taskMetrics.getOptimalAllocation();
				long oldValue = slotMetrics.getOldMemSize();
				slotMetrics.setTargetMemSize(target);
				if(target > oldValue){
					metrics.addExpand(slotMetrics);
				} else if(target < oldValue){
					metrics.addShrink(slotMetrics);
				}
				slots.remove(slotID);
			}
		}

		// Set for stateless slot
		for(String slot: slots){
			SlotMetrics slotMetrics = metrics.getSlot(slot);
			if(slotMetrics.getOldMemSize() > 0L){
				slotMetrics.setTargetMemSize(0L);
				metrics.addShrink(slotMetrics);
			}
		}
	}

	@Override
	public void checkScheduleDone(SlotID slotID){
		lock.lock();
		List<SlotMetrics> shrinks = metrics.getShrink();
		List<SlotMetrics> expands = metrics.getExpand();
		for(SlotMetrics slot : shrinks){
			if(slot.getSlot().getSlotId().equals(slotID)){
				shrinks.remove(slot);
				break;
			}
		}
		for(SlotMetrics slot : expands){
			if(slot.getSlot().getSlotId().equals(slotID)){
				expands.remove(slot);
				break;
			}
		}
		if(shrinks.size() == 0 && expands.size() == 0){
			scheduleTime = System.currentTimeMillis();
			onScheduling = false;
			LOG.info("All slots are resized successfully!!!");
		}
		lock.unlock();
	}

	private class TestingThread extends Thread {

		public boolean init(){
			mRetriever.init();
			boolean flag = updateSlotInfo();
			cacheSizeInit();
			algorithm.init();
			return flag;
		}

		@Override
		public void run() {

			LOG.info("Elastic Memory Manager Test");
			System.out.println("Elastic Memory Manager Test");

			try {
				// todo, if the time of sleep is too short, may cause receiving not belong key
				Thread.sleep(30000);
				boolean flag = init();

				Thread.sleep(6*60*1000);
				long warnUp = 4*60*1000;

				long now = System.currentTimeMillis();
				scheduleTime = now;
				long warnUpStart = now, metricsTimeFine = now, metricsTimeCoarse = now;
				while (flag) {
					while(System.currentTimeMillis() - metricsTimeFine <= metricsIntervalFine){}

					if(!onScheduling) {
						now = System.currentTimeMillis();

						if(now - metricsTimeFine > metricsIntervalFine) {
							mRetriever.collectMetricsFine();
							metricsTimeFine = now;
						}

						boolean success = false;
						if(now - metricsTimeCoarse > metricsIntervalCoarse){
							success = mRetriever.updateMetricsCoarse();
							metricsTimeCoarse = now;
						}

						if(success && (now - scheduleTime >= scheduleInterval) && (System.currentTimeMillis() - warnUpStart >= warnUp)){
							algorithm.startExec();
							onScheduling = true;
							startResize();
						}

						printBasicStateful();
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
