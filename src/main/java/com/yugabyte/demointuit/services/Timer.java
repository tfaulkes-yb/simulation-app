package com.yugabyte.demointuit.services;

public interface Timer {
	public Timer start();
	public Timer timeSubPortion(String description);
	public void end(ExecutionStatus status);
}
