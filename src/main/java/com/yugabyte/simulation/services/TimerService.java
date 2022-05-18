package com.yugabyte.simulation.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

import com.yugabyte.simulation.dao.TimerResult;
import com.yugabyte.simulation.dao.WorkloadDesc;

@Service
public class TimerService {
	
	private class TimerImpl implements Timer {
		private final TimerType type;
		private final List<SubPartTime> subPartsTimes = new ArrayList<>();
		
		public TimerImpl(TimerType type) {
			this.type = type;
		}
		private long startTime;
		
		@Override
		public Timer start() {
			this.startTime = System.nanoTime();
			return this;
		}

		@Override
		public Timer timeSubPortion(String description) {
			this.subPartsTimes.add(new SubPartTime(description,
					System.nanoTime() - startTime));
			return this;
		}
		
		@Override
		public long end(ExecutionStatus status) {
			long time = System.nanoTime() - startTime;
			TimerService.this.submitResult(time/1000, type, status);
			return time;
		}
	}

	private final Map<TimerType, List<TimerResult>> timingResults;
	private static final int MAX_RESULTS_PER_SECOND = 250000;
	private static final int MAX_RESULTS_SECONDS = 3600;
	private class ResultsAccumulator {
		boolean use1stResult = true;
		long[][] currentSuccesses;
		long[][] currentFailures;
		int[] successCount;
		int[] failureCount;
		
		public ResultsAccumulator() {
			currentSuccesses = new long[2][];
			currentSuccesses[0] = new long[MAX_RESULTS_PER_SECOND];
			currentSuccesses[1] = new long[MAX_RESULTS_PER_SECOND];
			currentFailures = new long[2][];
			currentFailures[0] = new long[MAX_RESULTS_PER_SECOND];
			currentFailures[1] = new long[MAX_RESULTS_PER_SECOND];
			successCount = new int[] {0, 0};
			failureCount = new int[] {0, 0};
		}

		int getCurrentIndex() {
			return use1stResult ? 0 : 1;
		}
		
		void swapIndexAndZero() {
			this.use1stResult = !use1stResult;
			int newIndex = getCurrentIndex();
			successCount[newIndex] = 0;
			failureCount[newIndex] = 0;
		}
	}
	
	private Map<TimerType, ResultsAccumulator> accumulators;
	private Map<Long, Long> threadStartTimes = new ConcurrentHashMap<Long, Long>();
	private WorkloadDesc activeWorkload = null;
	
	private class ResultsCollator implements Runnable {
		private long startTime;
		@Override
		public void run() {
			this.startTime = System.currentTimeMillis();
			while (true) {
				long sampleStartTIme = System.currentTimeMillis();
				try {
					Thread.sleep(1000);
				}
				catch (InterruptedException ie) {
					break;
				}
				// Need to swap the results over.
				long now = System.currentTimeMillis();
				for (TimerType thisType : accumulators.keySet()) {
					ResultsAccumulator thisAccumulator = accumulators.get(thisType);
					int index = thisAccumulator.getCurrentIndex();
					synchronized (thisAccumulator) {
						thisAccumulator.swapIndexAndZero();
					}
					TimerResult result = new TimerResult(
							thisAccumulator.currentSuccesses[index],
							thisAccumulator.successCount[index],
							thisAccumulator.currentFailures[index], 
							thisAccumulator.failureCount[index],
							sampleStartTIme);
					
					WorkloadDesc workload = activeWorkload;
					String name = thisType.toString();
					if (workload != null && workload.getWorkloadName(thisType) != null) {
						name = workload.getWorkloadName(thisType);
						
						System.out.printf("%,dms: %s: %s", 
								now - startTime,
								name,
								result.toString());

					}
					
					
					List<TimerResult> results = timingResults.get(thisType);
					synchronized(results) {
						results.add(result);
						if (results.size() > MAX_RESULTS_SECONDS) {
							results.remove(0);
						}
					}
				}
			}
		}
	}
	
	public TimerService() {
		this.timingResults = new ConcurrentHashMap<TimerType, List<TimerResult>>();
		this.timingResults.put(TimerType.WORKLOAD2, new ArrayList<TimerResult>());
		this.timingResults.put(TimerType.WORKLOAD1, new ArrayList<TimerResult>());

		this.accumulators = new ConcurrentHashMap<TimerType, TimerService.ResultsAccumulator>();
		this.accumulators.put(TimerType.WORKLOAD2, new ResultsAccumulator());
		this.accumulators.put(TimerType.WORKLOAD1, new ResultsAccumulator());

		Thread collator = new Thread(new ResultsCollator());
		collator.setDaemon(true);
		collator.setName("Results collator");
		collator.start();
	}

	public synchronized TimerService setCurrentWorkload(WorkloadDesc workload) {
		this.activeWorkload = workload;
		return this;
	}
	
	public synchronized TimerService removeCurrentWorkload(WorkloadDesc workload) {
		if (this.activeWorkload == workload) {
			this.activeWorkload = null;
		}
		return this;
	}
	public Timer getTimer(TimerType type) {
		return new TimerImpl(type);
	}
	
	public Map<TimerType, List<TimerResult>> getResults(long fromTime) {
		TimerType thisType = TimerType.WORKLOAD2;
		List<TimerResult> results = timingResults.get(thisType);
		synchronized (results) {
			if (fromTime <= 0) {
				return this.timingResults;
			}
			else {
				// Return a sub-array containing the correct elements which
				// are greater than fromTime. Do a binary search for this.
				// Binary search for the right element
				int length = results.size();
				int start = 0;
				int end = length-1;
				int index = -1;
				while (start <= end) {
					int mid = (start + end)/2;
					// Move to the right side if the target is greater
					if (results.get(mid).getStartTimeMs() <= fromTime) {
						start = mid + 1;
					}
					else {
						// Move left side
						index = mid;
						end = mid - 1;
					}
				}
				// all types should have the same indexes so assume this.
				Map<TimerType, List<TimerResult>> timings = 
						new HashMap<TimerType, List<TimerResult>>();
				
				if (index > -1) {
					for (TimerType aType : accumulators.keySet()) {
						timings.put(aType, new ArrayList<TimerResult>(timingResults.get(aType).subList(index, length)));
					}
				}
				return timings;
			}
		}
	}
	
	private void submitResult(long timeInUs, TimerType type, ExecutionStatus status) {
		ResultsAccumulator accumulator = this.accumulators.get(type);
		synchronized (accumulator) {
			int index = accumulator.getCurrentIndex();
			int count;
			switch (status) {
			case SUCCESS:
				count = accumulator.successCount[index]; 
				if (count < MAX_RESULTS_PER_SECOND) {
					accumulator.currentSuccesses[index][count] = timeInUs;
					accumulator.successCount[index]++;
				}
				break;
			case ERROR:
				count = accumulator.failureCount[index]; 
				if (count < MAX_RESULTS_PER_SECOND) {
					accumulator.currentFailures[index][count] = timeInUs;
					accumulator.failureCount[index]++;
				}
				break;
			}
		}
	}
}
