package com.exchange.service;

import java.util.List;
import java.util.Map;

import com.exchange.model.MarketAmount;
import com.exchange.model.MarketPrice;
import com.exchange.model.Model;
import com.exchange.model.OCOModel;
import com.exchange.model.Offers;
import com.exchange.model.OrderBook;
import com.exchange.model.TradeBook;
import com.exchange.model.Trades;
import com.exchange.model.UserTrade;



public interface IService {

	List<UserTrade> offerPlace(Model model);
	OrderBook getOrderbook(String symbol,String limit);
	List<TradeBook> marketTrades(String symbol,String limit);
	MarketPrice getMarketPrice(String symbol,String action,Double requestamount);
	MarketAmount getMarketAmount(String symbol,String action,Double requestprice);
	UserTrade checkOrderStatus(String symbol,String offerid);
	UserTrade addAsset(Model model);
	UserTrade deleteAsset(Model model);
}

