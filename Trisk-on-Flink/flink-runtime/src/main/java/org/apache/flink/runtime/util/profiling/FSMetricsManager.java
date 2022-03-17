/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util.profiling;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * The MetricsManager is responsible for logging activity profiling information (except for messages).
 * It gathers start and end events for deserialization, processing, serialization, blocking on read and write buffers
 * and records activity durations. There is one MetricsManager instance per Task (operator instance).
 * The MetricsManager aggregates metrics in a {@link ProcessingStatus} object and outputs processing and output rates
 * periodically to a designated rates file.
 */
public class FSMetricsManager implements Serializable, MetricsManager {
	private static final Logger LOG = LoggerFactory.getLogger(FSMetricsManager.class);

	private String taskId; // Flink's task description
	private String workerName; // The task description string logged in the rates file
	private int instanceId; // The operator instance id
	private int numInstances; // The total number of instances for this operator
	private final JobVertexID jobVertexId; // To interact with StreamSwitch

	private long recordsIn = 0;	// Total number of records ingested since the last flush
	private long recordsOut = 0;	// Total number of records produced since the last flush
	private long usefulTime = 0;	// Total period of useful time since last flush
	private long processingTime = 0;
	private long waitingTime = 0;	// Total waiting time for input/output buffers since last flush
	private long latency = 0;	// Total end to end latency

	private long totalRecordsIn = 0;	// Total number of records ingested since the last flush
	private long totalRecordsOut = 0;	// Total number of records produced since the last flush

	private long currentWindowStart;

	private final ProcessingStatus status;

	private final long windowSize;	// The aggregation interval
//	private final String ratesPath;	// The file path where to output aggregated rates

	private long epoch = 0;	// The current aggregation interval. The MetricsManager outputs one rates file per epoch.

	private int nRecords;

	private final int numKeygroups;

	private HashMap<Integer, Long> kgLatencyMap = new HashMap<>(); // keygroup -> avgLatency
	private HashMap<Integer, Integer> kgNRecordsMap = new HashMap<>(); // keygroup -> nRecords
	private long lastTimeSlot = 0l;

	private final OutputStreamDecorator outputStreamDecorator;


	private final SubTaskLatencyMetrics latencyMetrics;

	/**
	 * @param taskDescription the String describing the owner operator instance
	 * @param jobConfiguration this job's configuration
	 */
	public FSMetricsManager(String taskDescription, JobVertexID jobVertexId, Configuration jobConfiguration, TaskMetricGroup metricGroup, int idInModel, int maximumKeygroups) {
		numKeygroups = maximumKeygroups;

		taskId = taskDescription;
		String workerId = taskId.replace("Timestamps/Watermarks", "Timestamps-Watermarks");
		workerName = workerId.substring(0, workerId.indexOf("(")-1);
		instanceId = Integer.parseInt(workerId.substring(workerId.lastIndexOf("(")+1, workerId.lastIndexOf("/"))) - 1; // need to consistent with partitionAssignment
		instanceId = idInModel;
//		System.out.println("----updated task with instance id is: " + workerName + "-" + instanceId);
		System.out.println("start execution: " + workerName + "-" + instanceId + " time: " + System.currentTimeMillis());
		numInstances = Integer.parseInt(workerId.substring(workerId.lastIndexOf("/")+1, workerId.lastIndexOf(")")));
		status = new ProcessingStatus();

		windowSize = jobConfiguration.getLong("policy.windowSize",  1_000_000_000L);
		nRecords = jobConfiguration.getInteger("policy.metrics.nrecords", 15);

		currentWindowStart = status.getProcessingStart();

		this.jobVertexId = jobVertexId;

		OutputStream outputStream;
		try {
			String expDir = jobConfiguration.getString("trisk.exp.dir", "/data/flink");
			outputStream = new FileOutputStream(expDir + "/trisk/" + getJobVertexId() + ".output");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			outputStream = System.out;
		}
		LOG.info("###### " + getJobVertexId() + " new task created");
		outputStreamDecorator = new OutputStreamDecorator(outputStream);
		this.latencyMetrics = new SubTaskLatencyMetrics(metricGroup);
	}

	public void updateTaskId(String taskDescription, Integer idInModel) {
//		synchronized (status) {
		LOG.info("###### Starting update task metricsmanager from " + workerName + "-" + instanceId + " to " + workerName + "-" + idInModel);
		// not only need to update task id, but also the states.
		if (idInModel == Integer.MAX_VALUE/2) {
			System.out.println("end execution: " + workerName + "-" + instanceId + " time: " + System.currentTimeMillis());
		} else if (instanceId != idInModel && instanceId == Integer.MAX_VALUE/2) {
			System.out.println("start execution: " + workerName + "-" + idInModel + " time: " + System.currentTimeMillis());
		}

		instanceId = idInModel; // need to consistent with partitionAssignment

		taskId = taskDescription;
		String workerId = taskId.replace("Timestamps/Watermarks", "Timestamps-Watermarks");
		numInstances = Integer.parseInt(workerId.substring(workerId.lastIndexOf("/") + 1, workerId.lastIndexOf(")")));

		status.reset();

		totalRecordsIn = 0;	// Total number of records ingested since the last flush
		totalRecordsOut = 0;	// Total number of records produced since the last flush

		// clear counters
		recordsIn = 0;
		recordsOut = 0;
		usefulTime = 0;
		currentWindowStart = 0;
		latency = 0;
		epoch = 0;

		LOG.info("###### End update task metricsmanager");
//		}
	}

	@Override
	public String getJobVertexId() {
		return workerName + "-" + instanceId;
	}

	/**
	 * Once the current input buffer has been consumed, calculate and log useful and waiting durations
	 * for this buffer.
	 * @param timestamp the end buffer timestamp
	 * @param processing total duration of processing for this buffer
	 * @param numRecords total number of records processed
	 */
	@Override
	public void inputBufferConsumed(long timestamp, long deserializationDuration, long processing, long numRecords, long endToEndLatency) {
		synchronized (status) {
			if (currentWindowStart == 0) {
				currentWindowStart = timestamp;
			}

			status.setProcessingEnd(timestamp);

			// aggregate the metrics
			recordsIn += numRecords;
			latency += endToEndLatency;
			recordsOut += status.getNumRecordsOut();
//			usefulTime += processing + deserializationDuration;
//				usefulTime += processing + status.getSerializationDuration()
//					- status.getWaitingForWriteBufferDuration();
			usefulTime += processing + status.getSerializationDuration() + deserializationDuration
//				+ status.getDeserializationDuration()
				- status.getWaitingForWriteBufferDuration();
			processingTime += processing;

//			outputStreamDecorator.println(String.format("%d+%d+%d-%d",
//				processing, status.getSerializationDuration(), deserializationDuration, status.getWaitingForWriteBufferDuration()));

			// clear status counters
			status.clearCounters();

			// if window size is reached => output
			if (timestamp - currentWindowStart > windowSize) {
				// compute rates
				long duration = timestamp - currentWindowStart;
				double trueProcessingRate = (recordsIn / (usefulTime / 1000.0)) * 1000000;
				double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000000;
				double observedProcessingRate = (recordsIn / (duration / 1000.0)) * 1000000;
				double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000000;
				outputStreamDecorator.println(latency + " : " + recordsIn);
				float tupleLatency = (float) latency / recordsIn;

				double utilization = (double) usefulTime / duration;
				double avgUsefulTime = 0.0;
				double avgProcessingTime = 0.0;
				if (recordsIn > 0) {
					avgUsefulTime = usefulTime / 1000000.0 / recordsIn;
					avgProcessingTime = processingTime / 1000000.0 / recordsIn;
				}

				// for network calculus
//				totalRecordsIn += recordsIn;
//				totalRecordsOut += recordsOut;

				StringBuilder keyGroupOutput = new StringBuilder();
				if (!status.outputKeyGroup.isEmpty()) {
					for (Map.Entry<Integer, Long> entry : status.outputKeyGroupState.entrySet()) {
						int partitionId = entry.getKey();
						keyGroupOutput.append(partitionId).append(":").append(status.outputKeyGroup.getOrDefault(partitionId, 0L)).append("&");
						totalRecordsOut += status.outputKeyGroup.getOrDefault(partitionId, 0L);
					}
					keyGroupOutput = new StringBuilder(keyGroupOutput.substring(0, keyGroupOutput.length() - 1));
				} else {
					keyGroupOutput = new StringBuilder("0");
				}

				StringBuilder keyGroupinput = new StringBuilder();
				if (!status.inputKeyGroup.isEmpty()) {
					for (Map.Entry<Integer, Long> entry : status.inputKeyGroupState.entrySet()) {
						int partitionId = entry.getKey();
						keyGroupinput.append(partitionId).append(":").append(status.inputKeyGroup.getOrDefault(partitionId, 0L)).append("&");

						totalRecordsIn += status.inputKeyGroup.getOrDefault(partitionId, 0L);
					}
					keyGroupinput = new StringBuilder(keyGroupinput.substring(0, keyGroupinput.length() - 1));
				} else {
					keyGroupinput = new StringBuilder("0");
				}
				latencyMetrics.setTupleLatency(tupleLatency);
				latencyMetrics.setQueuingTime(tupleLatency - avgUsefulTime);
				latencyMetrics.setServiceTime(avgUsefulTime);
				String ratesLine = jobVertexId + ","
					+ workerName + "-" + instanceId + ","
					+ " avgProcessingTime: " + avgProcessingTime + ","
					+ " endToEndLantecy: " + tupleLatency + ","
					+ " avgUsefulTime: " + avgUsefulTime + ","
					+ " recordsIn: " + recordsIn;


//				String ratesLine = jobVertexId + ","
//						+ workerName + "-" + instanceId + ","
//						+ " observedProcessingRate: " + observedProcessingRate + ","
//						+ " trueProcessingRate: " + trueProcessingRate + ","
////						+ " observedOutputRate: " + observedOutputRate + ","
////						+ " trueOutputRate: " + trueOutputRate + ","
//						+ " avgUsefulTime: " + avgUsefulTime + ","
//						+ " avgProcessingTime: " + avgProcessingTime + ","
//						+ " endToEndLantecy: " + endToEndLantecy + ","
//						+ " utilization: " + String.format("%.2f", utilization);
////						+ " totalRecordsIn: " + totalRecordsIn + ","
////						+ " totalRecordsOut: " + totalRecordsOut;

					//Issus: vScaling
					outputStreamDecorator.println(ratesLine);
//					System.out.println("workername: " + getJobVertexId() + " epoch: " + epoch + " keygroups: " + status.inputKeyGroup.keySet());


				// clear counters
				recordsIn = 0;
				recordsOut = 0;
				usefulTime = 0;
				processingTime = 0;
				currentWindowStart = 0;
				latency = 0;
				epoch++;
				// clear keygroups every time, because we are using delta here
//				status.clearKeygroups();
			}
		}
	}

	@Override
	public void groundTruth(int keyGroup, long arrivalTs, long completionTs) {
		outputStreamDecorator.println(String.format("ts: %d endToEnd latency: %d", arrivalTs, (completionTs - arrivalTs)));
	}

	@Override
	public void groundTruth(long arrivalTs, long latency) {
		//Issue: vScaling
		outputStreamDecorator.println(String.format("ts: %d endToEnd latency: %d", arrivalTs, latency));
//		System.out.printf("ts: %d endToEnd latency: %d%n", arrivalTs, latency);
	}

	/**
	 * A new input buffer has been retrieved with the given timestamp.
	 */
	@Override
	public void newInputBuffer(long timestamp) {
		status.setProcessingStart(timestamp);
		// the time between the end of the previous buffer processing and timestamp is "waiting for input" time
		status.setWaitingForReadBufferDuration(timestamp - status.getProcessingEnd());
	}

	@Override
	public void addSerialization(long serializationDuration) {
		status.addSerialization(serializationDuration);
	}

	@Override
	public void addDeserialization(long deserializationDuration) {
		status.addDeserialization(deserializationDuration);
	}

	@Override
	public void incRecordsOut() {
		status.incRecordsOut();
	}

	@Override
	public void incRecordsOutKeyGroup(int targetKeyGroup) {
		status.incRecordsOutChannel(targetKeyGroup);
	}

	@Override
	public void incRecordIn(int keyGroup) {
		status.incRecordsIn(keyGroup);
	}

	@Override
	public void addWaitingForWriteBufferDuration(long duration) {
		status.addWaitingForWriteBuffer(duration);

	}

	/**
	 * The source consumes no input, thus it must log metrics whenever it writes an output buffer.
	 * @param timestamp the timestamp when the current output buffer got full.
	 */
	@Override
	public void outputBufferFull(long timestamp) {
		if (taskId.contains("Source")) {

			synchronized (status) {

				if (currentWindowStart == 0) {
					currentWindowStart = timestamp;
				}

				setOutBufferStart(timestamp);

				// aggregate the metrics
				recordsOut += status.getNumRecordsOut();
				if (status.getWaitingForWriteBufferDuration() > 0) {
					waitingTime += status.getWaitingForWriteBufferDuration();
				}

				// clear status counters
				status.clearCounters();

				// if window size is reached => output
				if (timestamp - currentWindowStart > windowSize) {

					// compute rates
					long duration = timestamp - currentWindowStart;
					usefulTime = duration - waitingTime;
					double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000000;
					double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000000;
	//				totalRecordsOut += recordsOut;

					StringBuilder keyGroupOutput = new StringBuilder("");
					if (!status.outputKeyGroup.isEmpty()) {
						for (Map.Entry<Integer, Long> entry : status.outputKeyGroupState.entrySet()) {
							int partitionId = entry.getKey();
							keyGroupOutput.append(partitionId).append(":").append(status.outputKeyGroup.getOrDefault(partitionId, 0L)).append("&");
							totalRecordsOut += status.outputKeyGroup.getOrDefault(partitionId, 0L);
						}
						keyGroupOutput = new StringBuilder(keyGroupOutput.substring(0, keyGroupOutput.length() - 1));
					} else {
						keyGroupOutput = new StringBuilder("0");
					}

					String ratesLine = jobVertexId + ","
						+ workerName + "-" + instanceId + ","
						+ " observedOutputRate: " + observedOutputRate + ","
						+ " totalRecordsOut: " + totalRecordsOut + ","
						+ " trueOutputRate: " + trueOutputRate;

					outputStreamDecorator.println(ratesLine);

					// clear counters
					recordsOut = 0;
					usefulTime = 0;
					waitingTime = 0;
					currentWindowStart = 0;
					epoch++;
					// clear keygroups for delta
//					status.clearKeygroups();
				}
			}
		}
	}

	private void setOutBufferStart(long start) {
		status.setOutBufferStart(start);
	}


	@Override
	public void updateMetrics() {
		if (instanceId == Integer.MAX_VALUE/2) {
			return;
		}

		synchronized (status) {

			// compute rates
			// for network calculus
			totalRecordsIn += recordsIn;
			totalRecordsOut += recordsOut;


			// compute rates
			long duration = System.nanoTime() - currentWindowStart;
			double trueProcessingRate = (recordsIn / (usefulTime / 1000.0)) * 1000;
			double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000;
			double observedProcessingRate = (recordsIn / (duration / 1000.0)) * 1000;
			double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000;
			float endToEndLantecy = (float) latency/recordsIn;

			double utilization = (double) usefulTime / duration;

			StringBuilder keyGroupOutput = new StringBuilder();
			if (!status.outputKeyGroup.isEmpty()) {
				for (Map.Entry<Integer, Long> entry : status.outputKeyGroupState.entrySet()) {
					int partitionId = entry.getKey();
					keyGroupOutput.append(partitionId).append(":").append(status.outputKeyGroup.getOrDefault(partitionId, 0L)).append("&");
					totalRecordsOut += status.outputKeyGroup.getOrDefault(partitionId, 0L);
				}
				keyGroupOutput = new StringBuilder(keyGroupOutput.substring(0, keyGroupOutput.length() - 1));
			} else {
				keyGroupOutput = new StringBuilder("0");
			}

			StringBuilder keyGroupinput = new StringBuilder("");
			if (!status.inputKeyGroup.isEmpty()) {
				for (Map.Entry<Integer, Long> entry : status.inputKeyGroupState.entrySet()) {
					int partitionId = entry.getKey();
					keyGroupinput.append(partitionId).append(":").append(status.inputKeyGroup.getOrDefault(partitionId, 0L)).append("&");
					totalRecordsIn += status.inputKeyGroup.getOrDefault(partitionId, 0L);
				}
				keyGroupinput = new StringBuilder(keyGroupinput.substring(0, keyGroupinput.length() - 1));
			} else {
				keyGroupinput = new StringBuilder("0");
			}

			String ratesLine = jobVertexId + ","
				+ workerName + "-" + instanceId + ","
//						+ " trueProcessingRate: " + trueProcessingRate + ","
				+ " observedProcessingRate: " + observedProcessingRate;
//				+ ","
//				+ " endToEndLantecy: " + endToEndLantecy;
//						+ ","
//						+ " totalRecordsIn: " + totalRecordsIn + ","
//						+ " totalRecordsOut: " + totalRecordsOut;

			LOG.info("###### " + getJobVertexId() + ": receive barrier, dump current metrics");
			outputStreamDecorator.println(ratesLine);
			System.out.println(ratesLine);

			// clear counters
			recordsIn = 0;
			recordsOut = 0;
			usefulTime = 0;
			currentWindowStart = 0;
			latency = 0;
			epoch++;
			// clear keygroups for delta7
			status.clearKeygroups();
		}
	}

	public static class SubTaskLatencyMetrics {
		final MetricGroup metricGroup;

		private static final String SERVICE_TIME = "serviceTime";
		private static final String QUEUING_TIME = "queuingTime";
		private static final String TUPLE_LATENCY = "tupleLatency";

		protected final LatencyGauge serviceTimeGauge = new LatencyGauge();
		protected final LatencyGauge queueTimeGauge = new LatencyGauge();
		protected final LatencyGauge avgTupleLatencyGauge = new LatencyGauge();

		private SubTaskLatencyMetrics(MetricGroup metricGroup) {
			this.metricGroup = metricGroup;
			metricGroup.gauge(SERVICE_TIME, serviceTimeGauge);
			metricGroup.gauge(QUEUING_TIME, queueTimeGauge);
			metricGroup.gauge(TUPLE_LATENCY, avgTupleLatencyGauge);
		}

		public double getServiceTimeGauge() {
			return serviceTimeGauge.getValue();
		}

		public void setServiceTime(double time){
			serviceTimeGauge.setLatency(time);
		}

		public double getQueueTimeGauge() {
			return queueTimeGauge.getValue();
		}

		public void setQueuingTime(double time){
			queueTimeGauge.setLatency(time);
		}

		public double getAvgTupleLatencyGauge() {
			return avgTupleLatencyGauge.getValue();
		}

		public void setTupleLatency(double time){
			avgTupleLatencyGauge.setLatency(time);
		}
	}

	public static class LatencyGauge implements Gauge<Double> {

		private volatile double currentLatency = 0.0;

		public void setLatency(double latency) {
			currentLatency = latency;
		}

		@Override
		public Double getValue() {
			return currentLatency;
		}
	}

}
