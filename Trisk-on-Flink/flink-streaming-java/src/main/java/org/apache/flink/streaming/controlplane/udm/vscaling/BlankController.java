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
public class BlankController extends AbstractMemoryManager {
	private static final Logger LOG = LoggerFactory.getLogger(BlankController.class);

	private final TestingThread testingThread;
	private final String SIMPLE_TEST = "trisk.simple_test";
	private boolean simpleTest;

	private ReentrantLock lock = new ReentrantLock();

	public BlankController(ReconfigurationExecutor reconfigurationExecutor, Configuration configuration) {
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
				long metricsTimeFine = now, metricsTimeCoarse = now;
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
					printBasicStateful();
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
