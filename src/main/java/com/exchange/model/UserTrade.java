package com.exchange.model;

import com.fasterxml.jackson.annotation.JsonInclude;


public class UserTrade {

	private String matchedofferid;
	private String remaningqty;
	private String volume;
	private String tradeprice;
	private String message;
	private String requestamount;
	private String requestprice;
	private String dbinsert;
	private String symbol;
	private String action;
	private String matchedofferprice;
	private String offerid;
	private String tradeid;
	private String statuscode;
	private String ordertype;
	private String remarks;
	private int userTag;
	
	public int getUserTag() {
		return userTag;
	}
	public void setUserTag(int userTag) {
		this.userTag = userTag;
	}
	public String getOrdertype() {
		return ordertype;
	}
	public void setOrdertype(String ordertype) {
		this.ordertype = ordertype;
	}
	public String getRemarks() {
		return remarks;
	}
	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}
	
	public String getStatuscode() {
		return statuscode;
	}
	public void setStatuscode(String statuscode) {
		this.statuscode = statuscode;
	}
	public String getTradeid() {
		return tradeid;
	}
	public void setTradeid(String tradeid) {
		this.tradeid = tradeid;
	}
	public String getOfferid() {
		return offerid;
	}
	public void setOfferid(String offerid) {
		this.offerid = offerid;
	}
	public String getMatchedofferprice() {
		return matchedofferprice;
	}
	public void setMatchedofferprice(String matchedofferprice) {
		this.matchedofferprice = matchedofferprice;
	}
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public String getAction() {
		return action;
	}
	public void setAction(String action) {
		this.action = action;
	}
	public String getDbinsert() {
		return dbinsert;
	}
	public void setDbinsert(String dbinsert) {
		this.dbinsert = dbinsert;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}

	
	public String getMatchedofferid() {
		return matchedofferid;
	}
	public void setMatchedofferid(String matchedofferid) {
		this.matchedofferid = matchedofferid;
	}
	public String getRequestamount() {
		return requestamount;
	}
	public void setRequestamount(String requestamount) {
		this.requestamount = requestamount;
	}
	public String getRequestprice() {
		return requestprice;
	}
	public void setRequestprice(String requestprice) {
		this.requestprice = requestprice;
	}
	public String getRemaningqty() {
		return remaningqty;
	}
	public void setRemaningqty(String remaningqty) {
		this.remaningqty = remaningqty;
	}
	public String getVolume() {
		return volume;
	}
	public void setVolume(String volume) {
		this.volume = volume;
	}
	public String getTradeprice() {
		return tradeprice;
	}
	public void setTradeprice(String tradeprice) {
		this.tradeprice = tradeprice;
	}
	
}
