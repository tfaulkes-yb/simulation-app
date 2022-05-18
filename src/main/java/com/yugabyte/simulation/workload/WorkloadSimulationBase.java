package com.yugabyte.simulation.workload;


public class WorkloadSimulationBase {
	public static class Workload {
		private final String workloadId;
		private final long startTime;
		public Workload(WorkloadType type) {
			this.startTime = System.nanoTime();
			this.workloadId = type.getTypeName() + "_" + this.startTime;
		}
		
		public String getWorkloadId() {
			return workloadId;
		}
		
		public void terminate() {
			
		}
		
		public void start() {
			
		}
	}
}
