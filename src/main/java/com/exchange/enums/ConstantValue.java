package com.exchange.enums;

public enum ConstantValue {

	DRIVER("org.postgresql.Driver"), URL("jdbc:postgresql://localhost:5432/demo_matching-engine"),
	USERNAME("demo"), PASSWORD("demo"),
	BOOTSTRAPSERVERS("localhost:9092");

	private String value;

	ConstantValue(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

}
