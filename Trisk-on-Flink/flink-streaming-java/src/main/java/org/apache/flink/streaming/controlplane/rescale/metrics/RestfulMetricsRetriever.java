package org.apache.flink.streaming.controlplane.rescale.metrics;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import jdk.nashorn.internal.scripts.JO;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
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

	private String jid;

	public RestfulMetricsRetriever(String ip, int port, String jobName){
		rootAddress = "http://" + ip + ":" + port + "/v1";
		this.jobName = jobName;
	}

	public void init(){
		initJobId();
		System.out.println(jid);
	}

	private void initJobId(){
		String address = rootAddress + JOBS + OVERVIEW;
		JSONObject response = getMetrics(address);
		List<JSONObject> jobs = JSON.parseArray(response.getJSONArray("jobs").toJSONString(),JSONObject.class);
		System.out.println(jobs);
		for (JSONObject job : jobs) {
			System.out.println(job.getString("name"));
			if(job.getString("name").equals(this.jobName)){
				jid = job.getString("jid");
				break;
			}
		}
	}

	public void test(){

	}

	public JSONObject getMetrics(String address){
		JSONObject metrics = null;
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
			String msg="";
			if(code == 200){
				msg = new BufferedReader(new InputStreamReader((connection.getInputStream()))).lines().collect(Collectors.joining("\n"));
				metrics = JSONObject.parseObject(msg);
			} else {
				System.out.println("Return code is: " + code);
			}
			connection.disconnect();
			System.out.println("metrics retriever restful" + msg);

		} catch (IOException e){
			e.printStackTrace();
		}
		return metrics;
	}
}
