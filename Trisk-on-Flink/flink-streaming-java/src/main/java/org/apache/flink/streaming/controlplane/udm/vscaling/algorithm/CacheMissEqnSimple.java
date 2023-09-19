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

public class CacheMissEqnSimple implements Algorithm{

	private final VScalingMetrics metrics;
	private final String algorithmFile;
	private final String cheFile;
	private final String fittingFile;
	private final String basicInfoFile;
	private final String dataPath;
	private final String dataFile;
	private final String metricsFile;
	private final String cheDataFile;
	private final String resultFile;
	private final String DATA_DIRECT = "/data";

	private long exeTimes = 0;
	private long WarmUpTimes = 60;
	private final PythonAlgorithm cheModel;

	private Map<Integer, Map<Integer, Map<Double, Long>>> estimatedPoints = new HashMap<>();

	public CacheMissEqnSimple(VScalingMetrics metrics, String path){
		this.metrics = metrics;

		algorithmFile = path + "/algorithm.py";
		cheFile = path + "/che.py";
		fittingFile = path + "/fit";
		dataPath = path + DATA_DIRECT;
		recreatePath(path, DATA_DIRECT);

		basicInfoFile = dataPath + "/basic_info.properties";
		dataFile = dataPath + "/points";
		metricsFile = dataPath + "/metrics.data";
		cheDataFile = dataFile + "/points-from-che.data";
		resultFile = dataPath + "/result.data";

		cheModel = new PythonAlgorithm(metrics, path);
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
//		need to compile the c file first.
		compileFit();

		publishBasicInfo();
	}

	private void compileFit(){
		String[] cmd = new String[] {"gcc", fittingFile+".c", "-o", fittingFile, "-lm"};
		String msg = execScript(cmd);
		if (msg.contains("error")){
			System.out.println("Compile fit.c failed, msg: " + msg);
		}
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

		if (exeTimes < WarmUpTimes) {
			cheModel.startExec();
			exeTimes += 1;
		} else
			eqnModel();

	}

	private void eqnModel(){
		// Step1: publish metrics
		publishMetrics();

		// step2: fitting for CacheMissEqn
		startCacheMissEqn();

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
			"beta=" + metrics.betaToString() + "\n" +
			"state.size=" + metrics.stateSizesTaskToString() + "\n" +
			"item.frequency=" + metrics.itemFrequencyToString() + "\n";
		writeFile(metricsFile, stringBuilder);
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
				execFit(fileNameIn);

				String fileNameOut = fileNameIn + ".g";
				printCacheMissEqnLog(fileNameOut);
			}
			index ++;
		}
	}

	private void estimatedFromChe(){
		String[] cmdArr = new String[] {"python3", cheFile};
		String response = execScript(cmdArr);
		System.out.println(response);

		readEstimatedDate();
	}

	// data format: "operator index,task index,miss ratio,cache size
	private void readEstimatedDate(){
		String output = readFile(cheDataFile);

		estimatedPoints.clear();
		for(String line : output.split("\n")){
			String[] items = line.split(",");
			Integer operator = Integer.parseInt(items[0].trim());
			Integer task = Integer.parseInt(items[1].trim());
			Double missRatio = Double.parseDouble(items[2].trim());
			Long cacheSize = Long.parseLong(items[3].trim());

			Map<Integer, Map<Double, Long>> operatorMap = estimatedPoints.getOrDefault(operator, new HashMap<>());
			Map<Double, Long> taskMap = operatorMap.getOrDefault(task, new HashMap<>());

			taskMap.put(missRatio, cacheSize);
			operatorMap.put(task, taskMap);
			estimatedPoints.put(operator, operatorMap);
		}
	}

	private String execFit(String fileName){
		String exe = fittingFile;
		String[] cmdArr = new String[] {exe, fileName};
		return execScript(cmdArr);
	}

	private String execPythonFile(String file){
		String exe = "python3";
		String[] cmdArr = new String[] {exe, file};
		return execScript(cmdArr);
	}

	private String execScript(String[] command){
		String response = "";
		try {
			Process process = Runtime.getRuntime().exec(command);
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
