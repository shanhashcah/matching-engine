package com.exchange.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

public class OrderBook {
	
	List<Ask>ask;
	
	List<Bid>bid;
	public List<Ask> getAsk() {
		return ask;
	}
	@JsonInclude(JsonInclude.Include.ALWAYS)
	public void setAsk(List<Ask> ask) {
		this.ask = ask;
	}
	public List<Bid> getBid() {
		return bid;
	}
	@JsonInclude(JsonInclude.Include.ALWAYS)
	public void setBid(List<Bid> bid) {
		this.bid = bid;
	}
	
}
