package com.exchange.dao;

import java.util.List;

import com.exchange.model.Model;
import com.exchange.model.Offers;
import com.exchange.model.Symbols;
import com.exchange.model.TradeBook;
import com.exchange.model.Trades;

public interface IDao {

	List<Offers> allOffers();
	int insertOffers(Offers o);
	int deleteOffer(String offerid);
	Offers getOffer(String offerid);
	int offerAmountUpdate(String amount,String offerid);
	int insertTradeBook(TradeBook book);
	int insertTrades(Trades trades);
	int offerPriceUpdate(String price,String offerid);
	List<TradeBook> alltradebook(String symbol,int limit);
	String activeOffers();
	List<Symbols> getSymbol();
	int insertAsset(Model model);
	boolean checkAsset(int symbol,String pair_name);
	int deleteAsset(Model model);
	boolean checkOfferBySymbol(String symbol);
}
