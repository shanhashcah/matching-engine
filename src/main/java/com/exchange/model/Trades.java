package com.exchange.model;

import java.time.LocalDateTime;

//import org.springframework.data.cassandra.core.mapping.PrimaryKey;
//import org.springframework.data.cassandra.core.mapping.Table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter @Setter
@AllArgsConstructor
public class Trades {

	
	private String action;
	private String matchedofferid;
	private String matchedofferprice;
	private String symbol;
	private String tradeid;
	private String tradeprice;
	private String volume;	
	private String requestamount;
	private String requestprice;
	private String remaningqty;
	private String ordertype;
}
