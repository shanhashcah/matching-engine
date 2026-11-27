package com.exchange.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TradeBookChanges {

	private String quantity;
	private String price;
	private String symbol;
	private String timestamp;
	private String side;
}
