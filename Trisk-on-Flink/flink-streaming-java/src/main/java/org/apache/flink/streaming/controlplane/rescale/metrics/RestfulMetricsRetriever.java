package org.apache.flink.streaming.controlplane.rescale.metrics;

import akka.util.Switch;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import jdk.nashorn.internal.scripts.JO;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.awt.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

public class RestfulMetricsRetriever {

	final private String rootAddress;
	final private String jobName;

	final private String JOBS = "/jobs";
	final private String VERTICES = "/vertices";
	final private String SUBTASKS = "/subtasks";
	final private String TMS = "/taskmanagers";
	final private String OVERVIEW = "/overview";
	final private String METRICS = "/metrics";
	final private String PLAN = "/plan";
	final private int retry = 6;

	private String jid;

	final private Map<String, String> verticesName;
	final private Map<String, Integer> verticesSubTasks;
	final private Map<String, Map<Integer, Map<String, String>>> states;
	final private Map<String, Map<Integer, Map<String, Double>>> statesAccessTime;
	final private Map<String, Map<Integer, Map<String, ArrayList<Long>>>> statesItemFrequency;
	final private Map<String, Map<Integer, Map<String, Double>>> statesAvgStateSize;
	final private Map<String, Map<Integer, Double>> serviceTime;
	final private Map<String, Map<Integer, Double>> queuingTime;
	final private Map<String, Map<Integer, Double>> tupleLatency;

	public RestfulMetricsRetriever(String ip, int port, String jobName){
		rootAddress = "http://" + ip + ":" + port + "/v1";
		this.jobName = jobName;
		this.verticesName = new HashMap<>();
		this.verticesSubTasks = new HashMap<>();
		this.states = new HashMap<>();
		this.statesAccessTime = new HashMap<>();
		this.statesItemFrequency = new HashMap<>();
		this.statesAvgStateSize = new HashMap<>();
		this.serviceTime = new HashMap<>();
		this.queuingTime = new HashMap<>();
		this.tupleLatency = new HashMap<>();
	}

	public void init(){
		int tries = 0;
		try {
			while(tries < retry && jid == null) {
				initJobId();
				tries++;
				Thread.sleep(1000);
			}
			while(tries < retry && verticesName.size() == 0){
				initVertices();
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
			System.out.println(states);
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

	private void initVertices(){
		String address = rootAddress + JOBS + "/" + jid + PLAN;
		JSONObject response = getMetricsJson(address);
		List<JSONObject> nodes = filterJsonArray(response.getJSONObject("plan").getJSONArray("nodes"));
		for(JSONObject node : nodes){
			String id = node.getString("id");
			verticesName.put(id, node.getString("description"));
			verticesSubTasks.put(id, node.getInteger("parallelism"));
		}
	}

	private boolean initMetrics(){
		for(Map.Entry<String, Integer> entry : verticesSubTasks.entrySet()){
			Map<Integer, Map<String, String>> verticesStates = new HashMap<>();
			String vertices = entry.getKey();

			for(int i = 0; i < entry.getValue(); i ++){
				Map<String, String> subTaskStates = new HashMap<>();

				String address = rootAddress + JOBS + "/" + jid + VERTICES + "/" + vertices + SUBTASKS + "/" + i + METRICS;
				System.out.println(address);
				String response = getMetrics(address);
				List<JSONObject> metrics = JSON.parseArray(response,JSONObject.class);

				if(metrics.size() <= 0){
					return false;
				}
				for(JSONObject metric : metrics){
					String metricID = metric.getString("id");
					String[] metricIDFilter = metricID.split("\\.");
//					System.out.println("metricIDFilter: " + Arrays.toString(metricIDFilter));
					if(metricIDFilter.length >= 4 && metricIDFilter[1].equals("state_name")){
						String stateName = metricIDFilter[2];
						String stateType = null;
						System.out.println("stateType: " + metricIDFilter[3]);
						if(metricIDFilter[3].contains("mapState"))
							stateType = "mapStateCacheAccessLatency";
						else if(metricIDFilter[3].contains("aggregatingState"))
							stateType = "aggregatingStateGetLatency";
						else if(metricIDFilter[3].contains("reducingState"))
							stateType = "reducingStateGetLatency";
						else if(metricIDFilter[3].contains("listState"))
							stateType = "listStateGetLatency";
						else if(metricIDFilter[3].contains("valueState"))
							stateType = "valueStateGetLatency";
						if(stateType != null)
							subTaskStates.put(stateName, stateType);
					}
				}
				verticesStates.put(i, subTaskStates);
			}
			states.put(vertices, verticesStates);
		}
		System.out.println("states: " + states);
		return true;
	}

	public void updateMetrics(){
		updateSubTaskMetrics();
//		update
	}

	private void updateSubTaskMetrics(){
		statesAccessTime.clear();
		serviceTime.clear();
		queuingTime.clear();
		tupleLatency.clear();
		statesItemFrequency.clear();
		statesAvgStateSize.clear();
		for(Map.Entry<String, Map<Integer, Map<String, String>>> entry : states.entrySet()){
			Map<Integer, Map<String, Double>> verticesSATime = new HashMap<>();
			Map<Integer, Double> verticesServiceTime = new HashMap<>();
			Map<Integer, Double> verticesQueuingTime = new HashMap<>();
			Map<Integer, Double> verticesTupleLatency = new HashMap<>();
			Map<Integer, Map<String, ArrayList<Long>>> verticesItemFreq = new HashMap<>();
			Map<Integer, Map<String, Double>> verticesStateSize = new HashMap<>();

			String vertices = entry.getKey();
			for(Map.Entry<Integer, Map<String, String>> entry2 : entry.getValue().entrySet()){
				Integer subTaskIndex = entry2.getKey();

				// 1. get state access time
				Map<String, Double> subTaskSATime = new HashMap<>();
				Map<String, ArrayList<Long>> subTaskItemFreq = new HashMap<>();
				Map<String, Double> subTaskStateSize = new HashMap<>();

				for(Map.Entry<String, String> entry3 : entry2.getValue().entrySet()){
					String stateName = entry3.getKey();
					String stateType = entry3.getValue();

					// 1. get state access time
					String metricsIDSATime = verticesName.get(vertices).replace(" ", "_") + ".state_name." + stateName + "." + stateType + "_mean";
					subTaskSATime.put(stateName, getSubTaskDoubleMetrics(vertices, subTaskIndex, metricsIDSATime));

					// 2. get item frequency
					String metricsIDIF = verticesName.get(vertices).replace(" ", "_") + ".state_name." + stateName + "." + "itemFrequency";
					subTaskItemFreq.put(stateName, getSubTaskListMetrics(vertices, subTaskIndex, metricsIDIF));

					// 3. get average state size
					String metricsID = verticesName.get(vertices).replace(" ", "_") + ".state_name." + stateName + "." + "stateSize";
					subTaskStateSize.put(stateName, getSubTaskDoubleMetrics(vertices, subTaskIndex, metricsID));
				}

				verticesSATime.put(subTaskIndex, subTaskSATime);
				verticesItemFreq.put(subTaskIndex, subTaskItemFreq);
				verticesStateSize.put(subTaskIndex, subTaskStateSize);

				// 4. get service time
				String metricsID = "serviceTime";
				verticesServiceTime.put(subTaskIndex, getSubTaskDoubleMetrics(vertices, subTaskIndex, metricsID));

				// 5. get queuing time
				metricsID = "queuingTime";
				verticesQueuingTime.put(subTaskIndex, getSubTaskDoubleMetrics(vertices, subTaskIndex, metricsID));

				// 6. get tuple latency
				metricsID = "tupleLatency";
				verticesTupleLatency.put(subTaskIndex, getSubTaskDoubleMetrics(vertices, subTaskIndex, metricsID));
			}

			statesAccessTime.put(vertices, verticesSATime);
			serviceTime.put(vertices, verticesServiceTime);
			queuingTime.put(vertices, verticesQueuingTime);
			tupleLatency.put(vertices, verticesTupleLatency);
			statesItemFrequency.put(vertices, verticesItemFreq);
			statesAvgStateSize.put(vertices, verticesStateSize);

		}
		System.out.println("state access time: " + statesAccessTime);
		System.out.println("service time: " + serviceTime);
		System.out.println("queuing time: " + queuingTime);
		System.out.println("tuple latency: " + tupleLatency);
		System.out.println("state item frequency: " + statesItemFrequency);
		System.out.println("state average state size: " + statesAvgStateSize);
	}

	private ArrayList<Long> getSubTaskListMetrics(String vertices, int subTaskIndex, String metrics){
		String address = combineMetricsURL(vertices, subTaskIndex, metrics);
		List<JSONObject> response = getMetricsJsonArray(address);
		ArrayList<Long> list = new ArrayList<>();
		for (JSONObject item : response){
			String listStr = item.getString("value");
			System.out.println("list string: " + listStr);
			System.out.println("substring: " + listStr.substring(1, listStr.length() - 1));
			String[] values = listStr.substring(1, listStr.length() - 1).split(", ");
			for (String value : values) {
				list.add(Long.parseLong(value));
			}
			break;
		}
		return list;
	}

	private Double getSubTaskDoubleMetrics(String vertices, int subTaskIndex, String metrics){
		String address = combineMetricsURL(vertices, subTaskIndex, metrics);
		List<JSONObject> response = getMetricsJsonArray(address);
		Double value = 0.0;
		for(JSONObject item : response){
			value = item.getDouble("value");
		}
		return value;
	}

	private String combineMetricsURL(String vertices, int subTaskIndex, String metrics){
		return rootAddress + JOBS + "/" + jid + VERTICES +
			"/" + vertices + SUBTASKS + "/" + subTaskIndex + METRICS + "?get=" + metrics;
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
//			System.out.println("metrics retriever restful: " + msg);

		} catch (IOException e){
			e.printStackTrace();
		}
		return msg;
	}
}
