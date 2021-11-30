package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.resourcemanager.slotmanager.TaskManagerSlot;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.TriskWithLock;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

public class VerticalScalingTest extends AbstractController {
	private static final Logger LOG = LoggerFactory.getLogger(VerticalScalingTest.class);

	private final Object object = new Object();
	private final TestingThread testingThread;

	public VerticalScalingTest(ReconfigurationExecutor reconfigurationExecutor) {
		super(reconfigurationExecutor);
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

	private void vScalingTest(){
		LOG.info("Start Vertical Scaling Test.");
		Collection<TaskManagerSlot> allSlots = getReconfigurationExecutor().getAllSlots();
		TaskManagerSlot slotTest = allSlots.iterator().next();
		MemorySize mem = new MemorySize(1024*1024*100);
		ResourceProfile target = ResourceProfile.newBuilder()
			.setCpuCores(1).setManagedMemory(mem).build();
		getReconfigurationExecutor().updateSlotResource(slotTest.getSlotId(), target, new ReconfigurationExecutor.UpdateResourceCallback() {
			@Override
			public void callback(SlotID slotID) {
				System.out.println("Resource Update Successfully for slot: " + slotID.toString());
			}
		});
	}

	private class TestingThread extends Thread {

		@Override
		public void run() {

			LOG.info("Before Vertical Scaling Test");
			// the testing jobGraph (workload) is in TestingWorkload.java, see that file to know how to use it.
			int statefulOpID = findOperatorByName("Splitter FlatMap");

			try {
				showOperatorInfo();
				// todo, if the time of sleep is too short, may cause receiving not belong key
				Thread.sleep(50000);
				vScalingTest();

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
