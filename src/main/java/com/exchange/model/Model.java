package com.exchange.model;

public class Model {
	
	private String symbol;
	private String amount;
	private String price;
	private String action;
	private String offerid;
	private String flag;
	private String activeoffers;
	private String ordertype;
	private String assetcode;
	private int userTag;
	
	
	
	public String getOrdertype() {
		return ordertype;
	}
	public int getUserTag() {
		return userTag;
	}
	public void setUserTag(int userTag) {
		this.userTag = userTag;
	}
	public void setOrdertype(String ordertype) {
		this.ordertype = ordertype;
	}
	public String getActiveoffers() {
		return activeoffers;
	}
	public void setActiveoffers(String activeoffers) {
		this.activeoffers = activeoffers;
	}
	public String getFlag() {
		return flag;
	}
	public void setFlag(String flag) {
		this.flag = flag;
	}
	public String getOfferid() {
		return offerid;
	}
	public void setOfferid(String offerid) {
		this.offerid = offerid;
	}
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public String getAmount() {
		return amount;
	}
	public void setAmount(String amount) {
		this.amount = amount;
	}
	public String getPrice() {
		return price;
	}
	public void setPrice(String price) {
		this.price = price;
	}
	public String getAction() {
		return action;
	}
	public void setAction(String action) {
		this.action = action;
	}
	@Override
	public String toString() {
		return "Model [symbol=" + symbol + ", amount=" + amount + ", price=" + price + ", action=" + action
				+ ", offerid=" + offerid + ", flag=" + flag + ", activeoffers=" + activeoffers + "]";
	}
	public String getAssetcode() {
		return assetcode;
	}
	public void setAssetcode(String assetcode) {
		this.assetcode = assetcode;
	}
	

}
