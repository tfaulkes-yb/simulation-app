package com.yugabyte.simulation.dao;

public class WorkloadParamDesc {
	private final String name;
	private final ParamType type;
	private final int minValue;
	private final int maxValue;
	private final ParamValue defaultValue;

	public WorkloadParamDesc(String name, ParamType type, int minValue, int maxValue, ParamValue defaultValue) {
		super();
		this.name = name;
		this.type = type;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.defaultValue = defaultValue;
	}
	
	public WorkloadParamDesc(String name, int minValue, int maxValue) {
		this(name, ParamType.NUMBER, minValue, maxValue, null);
	}

	public WorkloadParamDesc(String name, ParamType type, ParamValue defaultValue) {
		this(name, type, Integer.MIN_VALUE, Integer.MAX_VALUE, defaultValue);
	}
	
	public WorkloadParamDesc(String name, ParamType type) {
		this(name, type, null);
	}

	public WorkloadParamDesc(String name, int defaultValue) {
		this(name, Integer.MIN_VALUE, Integer.MAX_VALUE, defaultValue);
	}
	
	public WorkloadParamDesc(String name, boolean defaultValue) {
		this(name, ParamType.BOOLEAN, Integer.MIN_VALUE, Integer.MAX_VALUE, new ParamValue(defaultValue));
	}
	
	public WorkloadParamDesc(String name, String defaultValue) {
		this(name, ParamType.STRING, Integer.MIN_VALUE, Integer.MAX_VALUE, new ParamValue(defaultValue));
	}
	
	public WorkloadParamDesc(String name, int minValue, int maxValue, int defaultValue) {
		this(name, ParamType.NUMBER, minValue, maxValue, new ParamValue(defaultValue));
	}

	public String getName() {
		return name;
	}

	public ParamType getType() {
		return type;
	}

	public int getMinValue() {
		return minValue;
	}

	public int getMaxValue() {
		return maxValue;
	}
	
	public ParamValue getDefaultValue() {
		return defaultValue;
	}
}
