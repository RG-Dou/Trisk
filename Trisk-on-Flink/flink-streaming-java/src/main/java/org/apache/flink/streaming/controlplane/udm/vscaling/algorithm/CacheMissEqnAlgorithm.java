package org.apache.flink.streaming.controlplane.udm.vscaling.algorithm;

import org.apache.flink.streaming.controlplane.udm.vscaling.metrics.OperatorMetrics;
import org.apache.flink.streaming.controlplane.udm.vscaling.metrics.SlotMetrics;
import org.apache.flink.streaming.controlplane.udm.vscaling.metrics.TaskMetrics;
import org.apache.flink.streaming.controlplane.udm.vscaling.metrics.VScalingMetrics;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CacheMissEqnAlgorithm implements Algorithm{

	private final VScalingMetrics metrics;
	private final String algorithmFile;
	private final String fittingFile;
	private final String basicInfoFile;
	private final String dataPath;
	private final String dataFile;
	private final String metricsFile;
	private final String resultFile;

	private final String DATA_DIRECT = "/data";

	public CacheMissEqnAlgorithm(VScalingMetrics metrics, String path){
		this.metrics = metrics;

		algorithmFile = path + "/algorithm.py";
		fittingFile = path + "/fit";
		dataPath = path + DATA_DIRECT;
		recreatePath(path, DATA_DIRECT);

		basicInfoFile = dataPath + "/basic_info.properties";
		dataFile = dataPath + "/points";
		metricsFile = dataPath + "/metrics.data";
		resultFile = dataPath + "/result.data";
	}

	public void recreatePath(String path, String directoryName){
		File absPath = new File(path + directoryName);
		if(absPath.exists()) {
			File[] fileLeafs = absPath.listFiles();
			assert fileLeafs != null;
			for (File fileLeaf : fileLeafs) {
				fileLeaf.delete();
			}
			absPath.delete();
		}
		absPath.mkdir();
	}

	@Override
	public void init(){
		publishBasicInfo();
	}

	private void publishBasicInfo(){
		String stringBuilder = "[info]\n" +
			"operator.num=" + metrics.getNumOperator() + "\n" +
			"task.num=" + metrics.numTasksToString() + "\n" +
			"task.instance=" + metrics.taskInstancesToString() + "\n" +
			"memory.size=" + metrics.getTotalMem() + "\n";
		writeFile(basicInfoFile, stringBuilder);
	}

	@Override
	public void startExec(){
		if(!checkInputData())
			return;

		// step1: fitting for CacheMissEqn
		startCacheMissEqn();

		publishMetrics();

		String response = execPythonFile(algorithmFile);
		System.out.println(response);
		metrics.setAlgorithmInfo("success");

		if(metrics.getAlgorithmInfo().equals("success"))
			loadResult();
	}

	public boolean checkInputData(){
		Map<String, Long> maxAllocation = new HashMap<>();
		long totalMax = 0;
		long totalTasks = 0;
		for(String operatorID : metrics.getOperatorList()){
			OperatorMetrics operator = metrics.getOperator(operatorID);
			for(int taskIndex = 0; taskIndex < operator.getNumTasks(); taskIndex++){
				TaskMetrics task = operator.getTaskMetrics(taskIndex);
				double stateSize = task.getStateMetric().getStateSize();
				long totalItems = task.getStateMetric().getTotalItems();

				long maxSize = (long) (totalItems * stateSize);
				totalMax += maxSize;
				String slotID = task.getSlotID();
				System.out.println("Task: " + taskIndex + "total state: " + maxSize);
				maxAllocation.put(slotID, maxSize);
			}
			totalTasks += operator.getNumTasks();
		}
		if (totalMax <= metrics.getTotalMem()){
			long leftAvg = (metrics.getTotalMem() - totalMax)/totalTasks;
			for(Map.Entry<String, Long> entry : maxAllocation.entrySet()){
				SlotMetrics slot = metrics.getSlot(entry.getKey());
				long targetValue = leftAvg + entry.getValue();
				slot.setTargetMemSize(targetValue);
			}
			System.out.println("Total memory is enough to enable 100% hit ratio for all slots");
			return false;
		}
		return true;
	}

	private void startCacheMissEqn(){
		int index = 0;
		for (String operatorID : metrics.getOperatorList()){
			for (int taskID = 0; taskID < metrics.getOperator(operatorID).getNumTasks(); taskID ++){
				String data = metrics.cacheMissHistToString(operatorID, taskID);
				String fileNameIn = dataFile + "-" + index + "-" + taskID;
				// publish data
				writeFile(fileNameIn, data);

				// exe c script
				execScript(fileNameIn);

				String fileNameOut = fileNameIn + ".g";

				printCacheMissEqnLog(fileNameOut);
			}
			index ++;
		}
	}

	private void printCacheMissEqnLog(String fileName){
		String outs = readFile(fileName);
		for(String line : outs.split("\n")){
			if (line.contains("set title")){
				System.out.println("CacheMissEqn: " + line.split("\"")[1]);
			}
		}
	}

	private void publishMetrics(){
		String stringBuilder = "[data]\n" +
			"epoch=" + metrics.getEpoch() + "\n" +
			"frontEndTime=" + metrics.frontEndToString() + "\n" +
			"k=" + metrics.kToString() + "\n" +
			"backlog=" + metrics.backlogToString() + "\n" +
			"arrivalRate=" + metrics.arrivalRateToString() + "\n" +
			"alpha=" + metrics.alphaToString() + "\n" +
			"beta=" + metrics.betaToString() + "\n";
		writeFile(metricsFile, stringBuilder);
	}

	private void execScript(String fileName){

		try {
			String command = fittingFile + " " + fileName;
			Process process = Runtime.getRuntime().exec(command);
			process.waitFor(); // 等待脚本执行完成
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	private String execPythonFile(String file){

		String exe = "python3";
		String response = null;
		String[] cmdArr = new String[] {exe, file};

		try {
			Process process = Runtime.getRuntime().exec(cmdArr);
			InputStream is = process.getInputStream();
			DataInputStream dis = new DataInputStream(is);
			response = dis.readLine();
			process.waitFor();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		return response;
	}

	private void loadResult(){
		String[] result = readFile(resultFile).split("\n");
		long algorithmEpoch = Long.parseLong(result[0].split("=")[1]);
		if(algorithmEpoch != metrics.getEpoch()){
			System.out.println("Wrong Epoch");
			metrics.setAlgorithmInfo("fail");
			return;
		}

		String allocationStr = result[1].split("=")[1];
		int operatorIndex = 0;
		int taskIndex = 0;
		ArrayList<String> operators = metrics.getOperatorList();
		for(String item : allocationStr.substring(1, allocationStr.length() - 1).split(",")){
			if(item.length() <= 1) {
				continue;
			}
			OperatorMetrics operator = metrics.getOperator(operators.get(operatorIndex));
			operator.getTaskMetrics(taskIndex).setOptimalAllocation(Double.parseDouble(item));
			if (taskIndex >= operator.getNumTasks() - 1){
				operatorIndex += 1;
				taskIndex = 0;
			} else {
				taskIndex += 1;
			}
		}
	}

	private void writeFile(String fileName, String msg){
		try {
			File file = new File(fileName);
			if(!file.exists()){
				file.createNewFile();
			}
			FileOutputStream fos=new FileOutputStream(fileName);
			BufferedOutputStream bos=new BufferedOutputStream(fos);
			bos.write(msg.getBytes(),0,msg.getBytes().length);
			bos.flush();
			bos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String readFile(String fileName){
		File file = new File(fileName);
		StringBuilder result = new StringBuilder();
		try{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));

			String s = null;
			while((s = br.readLine())!=null){
				result.append(s).append("\n");
			}
			br.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return result.toString();
	}

}
