package com.exchange.controller;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.exchange.model.MarketAmount;
import com.exchange.model.MarketPrice;
import com.exchange.model.Model;
import com.exchange.model.OCOModel;
import com.exchange.model.Offers;
import com.exchange.model.OrderBook;
import com.exchange.model.TradeBook;
import com.exchange.model.Trades;
import com.exchange.model.UserTrade;
import com.exchange.service.ServiceImpl;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;



@RestController
public class HomeController {

	@Autowired
	ServiceImpl impl;
		
	@PostMapping("/offerPlace")
	List<UserTrade> OfferPlace(@RequestBody Model model)
	{
	  	return impl.offerPlace(model);
	}
	@RequestMapping(value={"/getOrderBook/{symbol}","/getOrderBook/{symbol}/{limit}"}, method = RequestMethod.GET)
	OrderBook offerBook(@PathVariable("symbol") String symbol,@PathVariable(value="limit",required = false) String limit)
	{
		
			return impl.getOrderbook(symbol,limit);
			
	}
	
	@RequestMapping(value= {"/marketTrades/{symbol}","/marketTrades/{symbol}/{limit}"},method = RequestMethod.GET)
	List<TradeBook>  marketTrades(@PathVariable("symbol") String symbol,@PathVariable(value="limit",required = false) String limit)
	{
	  	return impl.marketTrades(symbol,limit);
	}
	
	@RequestMapping("/marketPrice/{symbol}/{action}/{amount}")
	MarketPrice  marketTrades(@PathVariable("symbol") String symbol,@PathVariable("action") String action,@PathVariable("amount") String amount)
	{
	  	return impl.getMarketPrice(symbol, action,Double.parseDouble(amount));
	}
	
	@RequestMapping("/marketAmount/{symbol}/{action}/{price}")
	MarketAmount  marketAmount(@PathVariable("symbol") String symbol,@PathVariable("action") String action,@PathVariable("price") String price)
	{
	  	return impl.getMarketAmount(symbol, action,Double.parseDouble(price));
	}
	
	@GetMapping("/checkOrderStatus/{symbol}/{offerid}")
	public UserTrade orderStatus(@PathVariable("symbol") String symbol,@PathVariable("offerid") String offerid)
	{
		return impl.checkOrderStatus(symbol, offerid);
	}
	
	@PostMapping("/addAsset")
	public UserTrade addAsset(@RequestBody Model model)
	{
		return impl.addAsset(model);
	}
	@PostMapping("/deleteAsset")
	public UserTrade deleteAsset(@RequestBody Model model) {
		return impl.deleteAsset(model);
	}
}
