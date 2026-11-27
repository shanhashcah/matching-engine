package com.exchange.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderBookChanges {

	private String price;
	private String quantity;
	private String side;
	private String action;
	private String symbol;
}
