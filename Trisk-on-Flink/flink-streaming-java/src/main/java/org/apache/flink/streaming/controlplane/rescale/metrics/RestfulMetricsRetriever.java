package org.apache.flink.streaming.controlplane.rescale.metrics;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.controlplane.udm.vscaling.metrics.OperatorMetrics;
import org.apache.flink.streaming.controlplane.udm.vscaling.metrics.TaskMetrics;
import org.apache.flink.streaming.controlplane.udm.vscaling.metrics.VScalingMetrics;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

public class RestfulMetricsRetriever {

	final private String rootAddress;
	final private String jobName;
	final private String rocksdbLogDir;

	final private String JOBS = "/jobs";
	final private String VERTICES = "/vertices";
	final private String SUBTASKS = "/subtasks";
	final private String TMS = "/taskmanagers";
	final private String OVERVIEW = "/overview";
	final private String METRICS = "/metrics";
	final private String PLAN = "/plan";
	final private int retry = 100;

	private String jid;

	private final VScalingMetrics metrics;

	public RestfulMetricsRetriever(String ip, int port, String jobName, VScalingMetrics metrics, String rocksdbLogDir){
		rootAddress = "http://" + ip + ":" + port + "/v1";
		this.jobName = jobName;
		this.metrics = metrics;
		this.rocksdbLogDir = rocksdbLogDir;
	}

	public void init(){
		int tries = 0;
		try {
			while(tries < retry && jid == null) {
				initJobId();
				tries++;
				Thread.sleep(1000);
			}
			while(tries < retry && metrics.getFullNumOperator() == 0){
				initOperators();
				tries++;
				Thread.sleep(1000);
			}
			while(tries < retry && !initMetrics()){
				tries++;
				Thread.sleep(1000);
			}
			if (tries >= retry){
				System.out.println("exceed the maximum retries time!");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void initJobId(){
		String address = rootAddress + JOBS + OVERVIEW;
		JSONObject response = getMetricsJson(address);
		List<JSONObject> jobs = filterJsonArray(response.getJSONArray("jobs"));
		for (JSONObject job : jobs) {
			if(job.getString("name").equals(this.jobName)){
				jid = job.getString("jid");
				break;
			}
		}
	}

	private void initOperators(){
		String address = rootAddress + JOBS + "/" + jid + PLAN;
		JSONObject response = getMetricsJson(address);
		List<JSONObject> nodes = filterJsonArray(response.getJSONObject("plan").getJSONArray("nodes"));
		for(JSONObject node : nodes){
			String id = node.getString("id");

			metrics.addOperator(id, node.getString("description"));
			metrics.setNumTask(id, node.getInteger("parallelism"));
			System.out.println("Operator Name: "+ node.getString("description") + ", Operator ID: " + id + ", NumTask: " + node.getInteger("parallelism"));
		}
	}

	private boolean initMetrics(){
		for(String operatorId : metrics.getOperatorFullList()){
			int taskIndex = 0;

			String address = rootAddress + JOBS + "/" + jid + VERTICES + "/" + operatorId + SUBTASKS + "/" + taskIndex + METRICS;
			String response = getMetrics(address);
			List<JSONObject> metrics = JSON.parseArray(response,JSONObject.class);

			if(metrics.size() <= 0){
				return false;
			}
			for(JSONObject metric : metrics){
				String metricID = metric.getString("id");
				String[] metricIDFilter = metricID.split("\\.");
				if(metricIDFilter.length >= 4 && metricIDFilter[1].equals("state_name")){
					String stateName = metricIDFilter[2];
					String stateType = null;
					if(metricIDFilter[3].contains("mapStateCacheAccessLatency"))
						stateType = "mapStateCacheAccessLatency";
					else if(metricIDFilter[3].contains("aggregatingStateGetLatency"))
						stateType = "aggregatingStateGetLatency";
					else if(metricIDFilter[3].contains("reducingStateGetLatency"))
						stateType = "reducingStateGetLatency";
					else if(metricIDFilter[3].contains("listStateGetLatency"))
						stateType = "listStateGetLatency";
					else if(metricIDFilter[3].contains("valueStateGetLatency"))
						stateType = "valueStateGetLatency";
					if(stateType != null) {
						this.metrics.setStateName(operatorId, stateName);
						this.metrics.setStateAccessTimeTag(operatorId, stateType);
					}
				}
			}
		}
		return true;
	}

	// metrics that needs to be updated fine-grained includes State Access Time,
	public void collectMetricsFine(){
		for(String operatorId : metrics.getOperatorList()){
			Integer tasks = metrics.getNumTask(operatorId);
			String operatorName = metrics.getOperator(operatorId).getOperatorName();
			String stateName = metrics.getOperatorState(operatorId);
			for (int subTaskIndex = 0; subTaskIndex < tasks; subTaskIndex ++){
				if(stateName == null)
					break;
				// 1. get state access time
				String tag = this.metrics.getStateAccessTimeTag(operatorId);
				String metricsIDSATime = operatorName.replace(" ", "_") + ".state_name." + stateName + "." + tag + "_mean";
				Double accessTime = getSubTaskDoubleMetrics(operatorId, subTaskIndex, metricsIDSATime);
				if (accessTime != null) {
					metrics.setAccessTime(operatorId, subTaskIndex, accessTime / 1000000.0);
				}
			}
		}

		// all operators metrics
		for(String operatorID: metrics.getOperatorList()){
			OperatorMetrics operator = metrics.getOperator(operatorID);
			int tasks = operator.getNumTasks();
			for (int subTaskIndex = 0; subTaskIndex < tasks; subTaskIndex ++){
				// Checkpoint Alignment Time
				String metricsID = "checkpointAlignmentTime";
				Long alignmentTime = getSubTaskLongMetrics(operatorID, subTaskIndex, metricsID);
				if (alignmentTime != null) {
					metrics.setAlignmentTime(operatorID, subTaskIndex, alignmentTime);
				}

				// 4. get queuing delay
				metricsID = "queuingTime";
				Double queuingTime = getSubTaskDoubleMetrics(operatorID, subTaskIndex, metricsID);
				if(queuingTime != null) {
					metrics.setQueuingDelay(operatorID, subTaskIndex, queuingTime);
				}

				// 5. get service time
				metricsID = "serviceTime";
				Double serviceTime = getSubTaskDoubleMetrics(operatorID, subTaskIndex, metricsID);
				if (serviceTime != null) {
					metrics.setServiceTime(operatorID, subTaskIndex, serviceTime);
				}
			}
		}
	}

	public boolean updateMetricsCoarse(){
		metrics.incEpoch();
		boolean flag = collectOtherMetrics();
//		collectRocksdbStats();
		metrics.preprocess();
		return flag;
	}

	//ToDo: some metrics should be cleared after one scheduling.
	private boolean collectOtherMetrics(){

		// stateful metrics
		for(String operatorId : metrics.getOperatorList()){
			Integer tasks = metrics.getNumTask(operatorId);
			String operatorName = metrics.getOperator(operatorId).getOperatorName();
			String stateName = metrics.getOperatorState(operatorId);
			for (int subTaskIndex = 0; subTaskIndex < tasks; subTaskIndex ++){
				if(stateName == null)
					break;

				// 1. get average state size
				String metricsIDASS = operatorName.replace(" ", "_") + ".state_name." + stateName + "." + "stateSize";
				Tuple2<Double, Long> tuple2 = getSubTaskDTuple2Metrics(operatorId, subTaskIndex, metricsIDASS);
				if(tuple2 != null) {
					metrics.setStateSizesCounter(operatorId, subTaskIndex, stateName, tuple2.f1);
					metrics.setStateSizesState(operatorId, subTaskIndex, tuple2.f0);
				}

				// 2. get item frequency
				String metricsIDIF = operatorName.replace(" ", "_") + ".state_name." + stateName + "." + "itemFrequency";
				ArrayList<Long> itemFrequency;
				try {
					itemFrequency = getSubTaskListMetrics(operatorId, subTaskIndex, metricsIDIF);
				} catch (Exception e){
					return false;
				}
				if (itemFrequency != null) {
					metrics.setItemFrequency(operatorId, subTaskIndex, stateName, itemFrequency);
				}


				// 6. get number of records in
				String metricsID = "numRecordsIn";
				Long numRecordsIn = getSubTaskLongMetrics(operatorId, subTaskIndex, metricsID);
				if (numRecordsIn != null) {
					metrics.setNumRecordsIn(operatorId, subTaskIndex, numRecordsIn);
				}

				// 7. get arrival rate
				metricsID = "numRecordsInPerSecond";
				Double rate = getSubTaskDoubleMetrics(operatorId, subTaskIndex, metricsID);
				if (rate != null) {
					metrics.setArrivalRate(operatorId, subTaskIndex, rate);
				}

				metricsID = operatorName.replace(" ", "_") + ".state_name." + stateName + "." + "cacheDataHit";
				Long hit = getSubTaskLongMetrics(operatorId, subTaskIndex, metricsID);

				metricsID = operatorName.replace(" ", "_") + ".state_name." + stateName + "." + "cacheDataMiss";
				Long miss = getSubTaskLongMetrics(operatorId, subTaskIndex, metricsID);
				if(hit != null && miss != null) {
					metrics.setCacheHitMiss(operatorId, subTaskIndex, hit, miss);

					//append history data
					String slotID = metrics.getOperator(operatorId).getTaskMetrics(subTaskIndex).getSlotID();
					long cacheSize = metrics.getSlot(slotID).getOldMemSize();
					metrics.setStateHist(operatorId, subTaskIndex, cacheSize);
				}
			}
		}
		return true;
	}

	public void collectRocksdbStats(){
		for(String operatorID : metrics.getOperatorList()){
			OperatorMetrics operator = metrics.getOperator(operatorID);
			int numTasks = operator.getNumTasks();
			for (int taskIndex = 0; taskIndex < numTasks; taskIndex ++){
				TaskMetrics task = operator.getTaskMetrics(taskIndex);
				String fileName = getRocksdbFile(operatorID, task);
				if(fileName == null){
					System.out.println("Fail to fetch rocksdb file");
				} else {
					Tuple2<Long, Long> cacheHitMiss = getCacheHitMiss(fileName);
					task.setCacheHitMiss(cacheHitMiss.f0, cacheHitMiss.f1);
				}
 			}
		}
	}

	private String getRocksdbFile(String operatorID, TaskMetrics task){
		if (task.getRocksdbLogFileName() != null)
			return task.getRocksdbLogFileName();

		int taskIndex = task.getIndex() + 1;
		File file = new File(rocksdbLogDir);
		File[] fs = file.listFiles();
		assert fs != null;
		for(File f : fs){
			String name = f.getName();
			if(name.contains(jid) && name.contains(operatorID + "__" + taskIndex)){
				task.setRocksdbLogFileName(name);
				return name;
			}
		}
		return null;
	}

	private Tuple2<Long, Long> getCacheHitMiss(String fileName){
		File file = new File(rocksdbLogDir+fileName);
		long hit = 0;
		long miss = 0;
		try{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
			String s = null;
			while((s = br.readLine())!=null){
				if (s.contains("rocksdb.block.cache.data.hit")){
					hit = Long.parseLong(s.split(" : ")[1]);
				}
				if (s.contains("rocksdb.block.cache.data.miss")){
					miss = Long.parseLong(s.split(" : ")[1]);
				}
			}
			br.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return new Tuple2<>(hit, miss);
	}



	private Tuple2<Double, Long> getSubTaskDTuple2Metrics(String operatorId, int subTaskIndex, String metrics){
		String address = combineMetricsURL(operatorId, subTaskIndex, metrics);
		List<JSONObject> response = getMetricsJsonArray(address);
		if (response == null)
			return null;

		Tuple2<Double, Long> tuple2 = new Tuple2<>();
		for (JSONObject item : response) {
			String itemStr = item.getString("value");
			String[] values = itemStr.substring(1, itemStr.length() - 1).split(",");
			try {
				tuple2.f0 = Double.parseDouble(values[0]);
				tuple2.f1 = Long.parseLong(values[1]);
			} catch (Exception e){
				System.out.println("error on parse tuple2: " + itemStr);
			}
			break;
		}
		return tuple2;
	}

	private ArrayList<Long> getSubTaskListMetrics(String operatorId, int subTaskIndex, String metrics) throws Exception{
		String address = combineMetricsURL(operatorId, subTaskIndex, metrics);
//		System.out.println("address: " + address);
		List<JSONObject> response = getMetricsJsonArray(address);
		if (response == null)
			return null;

		ArrayList<Long> list = new ArrayList<>();
		for (JSONObject item : response){
			String listStr = item.getString("value");
			String[] values = listStr.substring(1, listStr.length() - 1).split(", ");
			for (String value : values) {
				try {
					list.add(Long.parseLong(value));
				} catch (Exception e){
					Log.info("address" + address +" error on metrics: " + metrics + " parse long: " + listStr);
					throw e;
				}
			}
			break;
		}
		return list;
	}

	private Double getSubTaskDoubleMetrics(String operatorId, int subTaskIndex, String metrics){
		String address = combineMetricsURL(operatorId, subTaskIndex, metrics);
		List<JSONObject> response = getMetricsJsonArray(address);
		if (response == null)
			return null;

		Double value = 0.0;
		for(JSONObject item : response){
			value = item.getDouble("value");
		}
		if(value == null || value.isNaN())
			value = 0.0;
		return value;
	}

	private Long getSubTaskLongMetrics(String operatorId, int subTaskIndex, String metrics){
		String address = combineMetricsURL(operatorId, subTaskIndex, metrics);
		List<JSONObject> response = getMetricsJsonArray(address);
		if (response == null)
			return null;

		long value = 0L;
		for(JSONObject item : response){
			value = item.getLongValue("value");
		}
		return value;
	}

	private Double getJobDoubleMetrics(String metrics){
		String address = combineJobMetricsURL(metrics);
		List<JSONObject> response = getMetricsJsonArray(address);
		Double value = 0.0;
		for(JSONObject item : response){
			value = item.getDouble("value");
		}
		return value;
	}

	private String combineMetricsURL(String operatorId, int subTaskIndex, String metrics){
		return rootAddress + JOBS + "/" + jid + VERTICES +
			"/" + operatorId + SUBTASKS + "/" + subTaskIndex + METRICS + "?get=" + metrics;
	}

	private String combineJobMetricsURL(String metrics){
		return rootAddress + JOBS + "/" + jid  + METRICS + "?get=" + metrics;
	}

	private List<JSONObject> filterJsonArray(JSONArray object){
		return JSON.parseArray(object.toJSONString(),JSONObject.class);
	}

	private List<JSONObject> getMetricsJsonArray(String address){
		return JSON.parseArray(getMetrics(address),JSONObject.class);
	}

	private JSONObject getMetricsJson(String address){
		return JSONObject.parseObject(getMetrics(address));
	}

	public String getMetrics(String address){
		String msg="";
		try{
			URL url = new URL(address);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setDoOutput(false);
			connection.setDoInput(true);
			connection.setRequestMethod("GET");
			connection.setUseCaches(true);
			connection.setInstanceFollowRedirects(true);
			connection.setConnectTimeout(3000);
			connection.connect();
			int code=connection.getResponseCode();
			if(code == 200){
				msg = new BufferedReader(new InputStreamReader((connection.getInputStream()))).lines().collect(Collectors.joining("\n"));
			} else {
				System.out.println("Return code is: " + code);
			}
			connection.disconnect();

		} catch (IOException e){
			e.printStackTrace();
		}
		return msg;
	}
}
