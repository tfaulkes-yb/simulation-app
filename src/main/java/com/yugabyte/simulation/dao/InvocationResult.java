package com.yugabyte.simulation.dao;

public class InvocationResult {
	private String data;

	public InvocationResult(String data) {
		super();
		this.data = data;
	}
	
	public String getData() {
		return data;
	}
}
