package org.apache.flink.streaming.controlplane.udm.vscaling;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.streaming.controlplane.udm.vscaling.metrics.SlotMetrics;
import org.apache.flink.streaming.controlplane.udm.vscaling.metrics.TaskMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class TestInitMemoryManager extends AbstractMemoryManager {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticMemoryManager.class);

	private final TestingThread testingThread;
	private final String SIMPLE_TEST = "trisk.simple_test";
	private boolean simpleTest;
	private final long adjustInterval = 60*1000;
	private boolean onScheduling = false;

	private ReentrantLock lock = new ReentrantLock();

	public TestInitMemoryManager(ReconfigurationExecutor reconfigurationExecutor, Configuration configuration) {
		super(reconfigurationExecutor, configuration);
		testingThread = new TestingThread();
		simpleTest = configuration.getBoolean(SIMPLE_TEST, false);
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

	private void adjustMemory(){
		for(Map.Entry<Integer, List<SlotMetrics>> entry : metrics.getSlotTMMap().entrySet()){
			List<SlotMetrics> slots = entry.getValue();
			for(SlotMetrics slot : slots){
				long oldSize = slot.getTargetMemSize();
				long newSize = oldSize + 10;
				slot.setTargetMemSize(newSize);
				for(TaskMetrics taskMetrics : slot.getTasks()){
					taskMetrics.setOptimalAllocation(newSize);
				}
				metrics.addExpand(slot);
			}
		}
		onScheduling = true;
		resizeGroup(new ArrayList<>(metrics.getExpand()));
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
			return flag;
		}

		@Override
		public void run() {

			LOG.info("Blank Controller Test");
			System.out.println("Blank Controller Test");

			try {
				// todo, if the time of sleep is too short, may cause receiving not belong key
				Thread.sleep(30000);
				boolean flag = init();
				Thread.sleep(6*60*1000);

				long now = System.currentTimeMillis();
				long metricsTimeFine = now, metricsTimeCoarse = now, adjustTime = now;
				while (flag) {
					while(System.currentTimeMillis() - metricsTimeFine <= metricsIntervalFine){}
					now = System.currentTimeMillis();

					if(now - metricsTimeFine > metricsIntervalFine) {
						mRetriever.collectMetricsFine();
						metricsTimeFine = now;
					}

					if(now - metricsTimeCoarse > metricsIntervalCoarse){
						mRetriever.updateMetricsCoarse();
						metricsTimeCoarse = now;
					}

					if(now - adjustTime > adjustInterval && !onScheduling){
						adjustMemory();
						adjustTime = now;
					}

					printBasicStateful();
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
