package com.exchange.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

public class OrderBookFullSnapshot {
	
	OrderBook snapshot;
	private String action;
	private String symbol;

	public OrderBook getSnapshot() {
		return snapshot;
	}
	public void setSnapshot(OrderBook snapshot) {
		this.snapshot = snapshot;
	}
	public String getAction() {
		return action;
	}
	public void setAction(String action) {
		this.action = action;
	}
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	
}
