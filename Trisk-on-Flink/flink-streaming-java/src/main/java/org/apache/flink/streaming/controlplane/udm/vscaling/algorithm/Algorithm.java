package org.apache.flink.streaming.controlplane.udm.vscaling.algorithm;

public interface Algorithm {

	public void init();

	public void startExec();
}
