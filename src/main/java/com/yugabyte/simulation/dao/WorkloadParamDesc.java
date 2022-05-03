package com.yugabyte.simulation.dao;

public class WorkloadParamDesc {
	private final String name;
	private final ParamType type;
	private final boolean required;
	private final int minValue;
	private final int maxValue;
	private final ParamValue defaultValue;

	public WorkloadParamDesc(String name, ParamType type, boolean required, int minValue, int maxValue, ParamValue defaultValue) {
		super();
		this.name = name;
		this.type = type;
		this.required = required;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.defaultValue = defaultValue;
	}
	
	public WorkloadParamDesc(String name, boolean required, int minValue, int maxValue) {
		this(name, ParamType.NUMBER, required, minValue, maxValue, null);
	}

	public WorkloadParamDesc(String name, ParamType type, boolean required, ParamValue defaultValue) {
		this(name, type, required, Integer.MIN_VALUE, Integer.MAX_VALUE, defaultValue);
	}
	
	public WorkloadParamDesc(String name, ParamType type, boolean required) {
		this(name, type, required, null);
	}

	public WorkloadParamDesc(String name, boolean required, int defaultValue) {
		this(name, required, Integer.MIN_VALUE, Integer.MAX_VALUE, defaultValue);
	}
	
	public WorkloadParamDesc(String name, boolean required, boolean defaultValue) {
		this(name, ParamType.BOOLEAN, required, Integer.MIN_VALUE, Integer.MAX_VALUE, new ParamValue(defaultValue));
	}
	
	public WorkloadParamDesc(String name, boolean required, String defaultValue) {
		this(name, ParamType.STRING, required, Integer.MIN_VALUE, Integer.MAX_VALUE, new ParamValue(defaultValue));
	}
	
	public WorkloadParamDesc(String name, boolean required, int minValue, int maxValue, int defaultValue) {
		this(name, ParamType.NUMBER, required, minValue, maxValue, new ParamValue(defaultValue));
	}

	public String getName() {
		return name;
	}

	public ParamType getType() {
		return type;
	}

	public boolean isRequired() {
		return required;
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
