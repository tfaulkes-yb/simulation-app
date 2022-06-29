package com.yugabyte.simulation.workload;

import com.yugabyte.simulation.exception.MultipleAggregationWorkloadException;
import com.yugabyte.simulation.services.TimerService;

public final class AggregationWorkloadType extends WorkloadType {

	private final class AggregationWorkloadInstanceType extends WorkloadTypeInstance {

		private static final AggregationWorkloadInstanceType instance = null; 
		public AggregationWorkloadInstanceType(TimerService timerService) {
			super(timerService);
			if (instance != null) {
				timerService.stopTimingWorkload(this);
				throw new MultipleAggregationWorkloadException();
			}
		}

		@Override
		protected String createWorkloadId() {
			return "Aggregation Counter";
		}
		
		@Override
		public WorkloadType getType() {
			return AggregationWorkloadType.this;
		}

		@Override
		public boolean isComplete() {
			return false;
		}
	}
	
	@Override
	public String getTypeName() {
		return "Aggregation Counter";
	}

	@Override
	public boolean canBeTerminated() {
		return false;
	}
	
	@Override
	public WorkloadTypeInstance createInstance(TimerService timerService, WorkloadManager workloadManager) {
		AggregationWorkloadInstanceType result = new AggregationWorkloadInstanceType(timerService);
		workloadManager.registerWorkloadInstance(result);
		return result;
	}

}
