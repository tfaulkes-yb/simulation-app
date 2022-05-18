package com.yugabyte.simulation.workload;

import com.yugabyte.simulation.services.ExecutionStatus;
import com.yugabyte.simulation.services.Timer;
import com.yugabyte.simulation.services.TimerService;
import com.yugabyte.simulation.services.TimerType;

public class FixedStepsWorkloadType extends WorkloadType {
	private final String[] steps;
	
	public interface ExecuteStep {
		public void run (int stepNum, String stepName);
	};
	
	public class FixedStepWorkloadInstance extends WorkloadTypeInstance {
		private final WorkloadStep[] workloadSteps;
		private final TimerService timerService;
		
		public FixedStepWorkloadInstance(TimerService timerService) {
			this.timerService = timerService;
			workloadSteps = new WorkloadStep[steps.length];
			for (int i = 0; i < steps.length; i++) {
				workloadSteps[i] = new WorkloadStep(steps[i]);
			}
		}
		
		@Override
		public WorkloadType getType() {
			return FixedStepsWorkloadType.this;
		}
		
		public WorkloadStep getCurrentStep() {
			int i;
			for (i = 0; i < workloadSteps.length && workloadSteps[i].isComplete(); i++);
			if (i < workloadSteps.length) {
				return workloadSteps[i];
			}
			else {
				return null;
			}
		}

		@Override
		public boolean isComplete() {
			return workloadSteps[workloadSteps.length-1].isComplete();
		}
		
		public void execute(ExecuteStep runner) {
			int step = 0;
			while (!isComplete() && !isTerminated()) {
				WorkloadStep currentStep = getCurrentStep();
				Timer timer = this.timerService.getTimer(TimerType.WORKLOAD1).start();
				try {
					runner.run(step, currentStep.getName());
					currentStep.complete(timer.end(ExecutionStatus.SUCCESS));
					step++;
					System.out.printf("Step %d (%s) completed in %fms\n", step, currentStep.getName(), currentStep.getTimeInNs()/1000000.0);
				}
				catch (Exception e) {
					this.setTerminatedByException(e);
					timer.end(ExecutionStatus.ERROR);
					throw e;
				}
			}
		}
	}

	public FixedStepsWorkloadType(String ... steps) {
		this.steps = steps;
	}
	
	@Override
	public String getTypeName() {
		return "FIXED";
	}

	@Override
	public FixedStepWorkloadInstance createInstance(TimerService timerService) {
		return new FixedStepWorkloadInstance(timerService);
	}
	
}