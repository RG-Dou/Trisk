package org.apache.flink.streaming.controlplane.udm.vscaling;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.resourcemanager.slotmanager.TaskManagerSlot;
import org.apache.flink.streaming.controlplane.rescale.metrics.RestfulMetricsRetriever;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.TriskWithLock;
import org.apache.flink.streaming.controlplane.udm.AbstractController;
import org.apache.flink.streaming.controlplane.udm.vscaling.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

// ToDo: comments and logs
public class ElasticMemoryManager extends AbstractController {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticMemoryManager.class);

	private final Object object = new Object();
	private final TestingThread testingThread;
	private final RestfulMetricsRetriever mRetriever;
	private final VScalingMetrics metrics;
	private final PythonAlgorithm algorithm;
	private final String REST_SERVER_IP = "trisk.config.rest_server_ip";
	private final String REST_SERVER_PORT = "rest.port";
	private final String JOB_NAME = "trisk.config.job_name";
	private final String SCHEDULE_INTERVAL = "trisk.config.schedule_interval";
	private final String METRICS_INTERVAL = "trisk.config.metrics_interval";
	//Todo: how to get total memeory;
	private final String TM_MANAGED_MEMORY = "trisk.taskmanager.managed_memory";
	private final String AlgorithmDataPath = "trisk.vScaling.python.path";
	private final String ROCKSDB_LOG_DIR = "state.backend.rocksdb.log.dir";

	private final String SIMPLE_TEST = "trisk.simple_test";

	private final Long scheduleInterval;
	private final Long metricsInterval;
	private boolean onScheduling = false;
	private long scheduleTime;

	private ReentrantLock lock = new ReentrantLock();

	public ElasticMemoryManager(ReconfigurationExecutor reconfigurationExecutor, Configuration configuration) {
		super(reconfigurationExecutor);
		String jobName = configuration.getString(JOB_NAME, "Nexmark Query");
		scheduleInterval = configuration.getLong(SCHEDULE_INTERVAL, 20000);
		metricsInterval = configuration.getLong(METRICS_INTERVAL, 1000);
		long totalMem = configuration.getLong(TM_MANAGED_MEMORY, 50);
		metrics = new VScalingMetrics(totalMem);
		String rocksdbLogDir = configuration.getString(ROCKSDB_LOG_DIR, "");
		mRetriever = new RestfulMetricsRetriever(configuration.getString(REST_SERVER_IP, "localhost"), configuration.getInteger(REST_SERVER_PORT, 8081), jobName, metrics, rocksdbLogDir);
		// ToDo: the python path should be a relative path: "Trisk-on-Flink/flink-tools/"
		String algorithmPath = configuration.getString(AlgorithmDataPath, "/home/drg/projects/work3/flink/alg-data/");
		algorithm = new PythonAlgorithm(metrics, algorithmPath);
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

	private void showOperatorInfo() {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();
		for (Iterator<OperatorDescriptor> it = streamJobState.getAllOperator(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			System.out.println(descriptor);
			System.out.println("key mapping:" + streamJobState.getKeyMapping(descriptor.getOperatorID()));
			System.out.println("key state allocation" + streamJobState.getKeyStateAllocation(descriptor.getOperatorID()));
			System.out.println("-------------------");
		}
	}

	private boolean updateSlotInfo(){
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

	public void cacheSizeInit(){
		for(Map.Entry<Integer, List<SlotMetrics>> entry : metrics.getSlotTMMap().entrySet()){
			List<SlotMetrics> slots = entry.getValue();
			long avgSize = metrics.getTotalMem() / slots.size();
			System.out.println("Init: average slot size " + avgSize + "M, total memory " + metrics.getTotalMem() + "M, for instance " + entry.getKey());
			for(SlotMetrics slot : slots){
				slot.setTargetMemSize(avgSize);
				metrics.addExpand(slot);
			}
		}
		resizeGroup(new ArrayList<>(metrics.getExpand()));
//		int totalNumTasks = 0;
//		for (String operatorID : metrics.getOperatorList()){
//			OperatorMetrics operator = metrics.getOperator(operatorID);
//			totalNumTasks += operator.getNumTasks();
//		}
//		long avgSize = metrics.getTotalMem();
//		if(totalNumTasks != 0)
//			avgSize = avgSize / totalNumTasks;
//		for (String operatorID : metrics.getOperatorList()){
//			OperatorMetrics operator = metrics.getOperator(operatorID);
//			int numTasks = operator.getNumTasks();
//			for(int taskIndex = 0; taskIndex < numTasks; taskIndex ++){
//				TaskMetrics task = operator.getTaskMetrics(taskIndex);
//				task.setOptimalAllocation(avgSize);
//				SlotMetrics slot = metrics.getSlot(task.getSlotID());
//				slot.setTargetMemSize(avgSize);
//				System.out.println("Init: operator: " + operator.getOperatorName() + ", task: " + taskIndex + ", target: " + avgSize);
//				metrics.addExpand(slot);
//			}
//		}
//		resizeGroup(metrics.getExpand());
	}

	private void startResize(){
//		grouping();
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

	private void resizeGroup(List<SlotMetrics> group){
		for(SlotMetrics slot : group){
			MemorySize mem = new MemorySize(slot.getTargetMemSize() * 1024 * 1024);
			//ToDo: how about the CPU cores
			ResourceProfile target = ResourceProfile.newBuilder()
				.setCpuCores(1).setManagedMemory(mem).build();
			resize(slot.getSlot().getSlotId(), target);
		}
	}

	private void resize(SlotID slotID, ResourceProfile resourceProfile){
		getReconfigurationExecutor().updateSlotResource(slotID, resourceProfile, new ReconfigurationExecutor.UpdateResourceCallback() {
			@Override
			public void callback(SlotID slotID) {
				onSuccessResize(slotID);
			}
		});
	}

	private void onSuccessResize(SlotID slotID){
		SlotMetrics slot = metrics.getSlot(slotID.toString());
		slot.setOldMemSize(slot.getTargetMemSize());
		LOG.info("Resource Update Successfully for slot: " + slotID);

		checkScheduleDone(slotID);
	}

	private void checkScheduleDone(SlotID slotID){
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


	public void printInAndOut(){
		for (String operatorID: metrics.getOperatorFullList()){
			OperatorMetrics operator = metrics.getOperator(operatorID);
			String name = operator.getOperatorName();
			if(name.contains("TimeAssigner")){
				int numTasks = operator.getNumTasks();
				for (int i = 0; i < numTasks; i ++){
					TaskMetrics task = operator.getTaskMetrics(i);
					long out = task.getRecordsIn();
					System.out.println("MetricsReport:" + System.currentTimeMillis() + ", operator:" + name + ", task:" + i + ", numRecordsOut:" + out);
				}
			}
		}
		for (String operatorID : metrics.getOperatorList()){
			OperatorMetrics operator = metrics.getOperator(operatorID);
			int numTasks = operator.getNumTasks();
			String operatorName = operator.getOperatorName();
			for (int i = 0; i < numTasks; i ++){
				TaskMetrics task = operator.getTaskMetrics(i);
				long in = task.getRecordsIn();
				System.out.println("MetricsReport:" + System.currentTimeMillis() + ", operator:" + operatorName + ", task:" + i + ", numRecordsIn:" + in);
			}
		}
	}


	public void printInfo(){
		for (String operatorID: metrics.getOperatorFullList()){
			OperatorMetrics operator = metrics.getOperator(operatorID);
			String name = operator.getOperatorName();
			if(Objects.equals(name, "Simple FlapMap----")){
				int numTasks = operator.getNumTasks();
				for (int i = 0; i < numTasks; i ++){
					TaskMetrics task = operator.getTaskMetrics(i);
					double queuingDelay = task.getQueuingTime();
					double alignmentTime = task.getAlignmentTime();
					System.out.println("MetricsReport:" + System.currentTimeMillis() + ", operator:" + name + ", queuingDelay:" + queuingDelay +
						", alignmentTime:" + alignmentTime);
				}
			}
		}
		for (String operatorID : metrics.getOperatorList()){
			OperatorMetrics operator = metrics.getOperator(operatorID);
			int numTasks = operator.getNumTasks();
			String operatorName = operator.getOperatorName();
			double k = operator.getk();
			double alpha = operator.getAlpha();
			double beta = operator.getBeta();
			double stateSize = operator.getStateSize();
			System.out.println("MetricsReport:" + System.currentTimeMillis() + ", operator:" + operatorName + ", k:" + k + ", alpha:" + alpha + ", beta:" + beta + ", stateSize:" + stateSize);
			for (int i = 0; i < numTasks; i ++){
				TaskMetrics task = operator.getTaskMetrics(i);
				double queuingDelay = task.getQueuingTime();
				double serviceTime = task.getServiceTime();
				double t = task.getFrontEndTime();
				double backlog = task.getBacklog();
				long optimal = (long) task.getOptimalAllocation();
				StateMetrics state = task.getStateMetric();
				double stateTime = state.getAccessTime();
				double hitRatio = state.getHitRatio();
				System.out.println("MetricsReport:" + System.currentTimeMillis() + ", operator:" + operatorName + ", task:" + i + ", queuingDelay:" + queuingDelay +
					", serviceTime:" + serviceTime + ", frontEndTime:" + t + ", backlog:" + backlog +
					", optimal:" + optimal + ", stateTime:" + stateTime + ", hitRatio:" + hitRatio);
			}
		}
	}

	private class TestingThread extends Thread {

		@Override
		public void run() {

			LOG.info("Before Vertical Scaling Test");

			try {
				// todo, if the time of sleep is too short, may cause receiving not belong key
				Thread.sleep(30000);
				boolean flag = true;
				mRetriever.init();
				flag = updateSlotInfo();
				cacheSizeInit();
				algorithm.init();
				Thread.sleep(9*60*1000);
				long warnUp = 3*60*1000;

				scheduleTime = System.currentTimeMillis();
				long warnUpStart = scheduleTime;
				long metricsTime = System.currentTimeMillis();
				while (flag) {
					while(System.currentTimeMillis() - metricsTime <= metricsInterval){}
					metricsTime = System.currentTimeMillis();
					if(!onScheduling) {
						mRetriever.collectMetrics();
						if(System.currentTimeMillis() - scheduleTime >= scheduleInterval){
							mRetriever.updateMetrics();

							// update KandB
//							algorithm.excCheApprox();
							// ToDo: Check whether metrics are collected correctly.
//							metrics.updateKandB();

							printInfo();
							if (System.currentTimeMillis() - warnUpStart >= warnUp) {
								algorithm.startExec();

								onScheduling = true;
								startResize();
							}
						}
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
