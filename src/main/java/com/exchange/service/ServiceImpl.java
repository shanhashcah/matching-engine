package com.exchange.service;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableLong;
import org.agrona.collections.MutableReference;
import org.json.JSONObject;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.exchange.dao.DaoImpl;
import com.exchange.model.Ask;
import com.exchange.model.Bid;
import com.exchange.model.MarketAmount;
import com.exchange.model.MarketPrice;
import com.exchange.model.Model;
import com.exchange.model.OCOModel;
import com.exchange.model.Offers;
import com.exchange.model.OrderBook;
import com.exchange.model.OrderBookChanges;
import com.exchange.model.OrderBookFullSnapshot;
import com.exchange.model.Symbols;
import com.exchange.model.TradeBook;
import com.exchange.model.TradeBookChanges;
import com.exchange.model.Trades;
import com.exchange.model.UserTrade;
//import com.exchange.repository.OfferRepository;
//import com.exchange.repository.TradeBookRepository;
//import com.exchange.repository.TradeRepository;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import exchange.core2.core.IEventsHandler;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.MatcherEventType;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.SymbolType;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.LoggingConfiguration;
import exchange.core2.core.orderbook.OrderBookNaiveImpl;
import jakarta.annotation.PostConstruct;
import exchange.core2.core.orderbook.IOrderBook;

@Service
public class ServiceImpl implements IService {

	@Autowired
	Environment env;

	@Autowired
	DaoImpl daoimpl;

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	DecimalFormat df2 = new DecimalFormat("#.########");

	String filename = null;

	int basecurrencyCode = 11;
	int quotecurrencyCode = 15;
	HashMap<String, OrderBookNaiveImpl> allorderbooks = new HashMap<String, OrderBookNaiveImpl>();

	protected CoreSymbolSpecification getCoreSymbolSpec(int symbol) {
		CoreSymbolSpecification symbolSpecXbtLtc = CoreSymbolSpecification.builder().symbolId(symbol) // symbol id
				.type(SymbolType.CURRENCY_EXCHANGE_PAIR).baseCurrency(basecurrencyCode) // base = satoshi (1E-8)
				.quoteCurrency(quotecurrencyCode) // quote = litoshi (1E-8)
				.baseScaleK(1_000_000L) // 1 lot = 1M satoshi (0.01 BTC)
				.quoteScaleK(10_000L) // 1 price step = 10K litoshi
				.takerFee(1900L) // taker fee 1900 litoshi per 1 lot
				.makerFee(700L) // maker fee 700 litoshi per 1 lot
				.build();
		return symbolSpecXbtLtc;

	}

	@PostConstruct
	public void offerRecovery() {

		filename = env.getProperty("filepath");

		System.out.println("Symbol");
		List<Symbols> allsymbols = daoimpl.getSymbol();

		for (int i = 0; i < allsymbols.size(); i++) {
			OrderBookNaiveImpl orderBook = new OrderBookNaiveImpl(getCoreSymbolSpec(allsymbols.get(i).getSymbol()),
					LoggingConfiguration.DEFAULT);
			allorderbooks.put(String.valueOf(allsymbols.get(i).getSymbol()), orderBook);
		}

		System.out.println("Symbol completed");

		System.out.println("===All Offers====");
		List<Offers> alloffer = daoimpl.allOffers();
		List<UserTrade> result = null;
		for (Offers o : alloffer) {
			System.out.print("OfferId.." + o.getOfferid() + " Symbol" + o.getSymbol() + " Amount" + o.getAmount()
					+ " Price" + o.getPrice() + " Action" + o.getAction() + " UserTag..."+ o.getUserTag());

			OrderBookNaiveImpl bookNaiveImpl = allorderbooks.get(o.getSymbol());
			if (bookNaiveImpl != null) {
				result = createOffer(o.getPrice(), o.getAmount(), Long.valueOf(o.getOfferid()), o.getAction(),
						o.getSymbol(), bookNaiveImpl, o.getUserTag(), true);
				if (result.get(0).getMessage() == "SUCCESS") {

					System.out.println("Offer Submitted Successfully.." + o.getOfferid());

				} else {
					System.out.println("Offer Submission Error");
				}
			}

		}

	}

	@Override
	public List<UserTrade> offerPlace(Model model) {

		filename = env.getProperty("filepath");

		List<UserTrade> result = null;
		long orderid = getOfferId();
		// writeOfferId();

		OrderBookNaiveImpl bookNaiveImpl = allorderbooks.get(model.getSymbol());
		if (bookNaiveImpl != null) {
			if (model.getOfferid() == null && model.getOrdertype() == null) {
				result = createOffer(model.getPrice(), model.getAmount(), orderid, model.getAction(), model.getSymbol(),
						allorderbooks.get(model.getSymbol()), model.getUserTag(),  false);
				return offerTradeCalculation(result);
			}

			else if (model.getFlag() != null) {
				if (model.getFlag().equalsIgnoreCase("cancel")) {
					return offerRemove(model, allorderbooks.get(model.getSymbol()));
				} else if (model.getFlag().equalsIgnoreCase("amt_modify")) {
					return offerReduce(model, allorderbooks.get(model.getSymbol()));
				}

			} else if (model.getOfferid() == null && model.getOrdertype() != null) {
				return createOfferIOCFOK(model.getPrice(), model.getAmount(), orderid, model.getAction(),
						model.getSymbol(), allorderbooks.get(model.getSymbol()), model.getOrdertype());
			}

			else {

				result = offerModify(model, allorderbooks.get(model.getSymbol()));
				return offerTradeCalculation(result);
			}
		} else {
			UserTrade res = new UserTrade();
			res.setMessage("Symbol is not available");
			res.setStatuscode("0");
			return Arrays.asList(res);
		}

		return null;

	}

	public String decimalStringToLong(String decimalString, int precision) {

		String[] splitStr = decimalString.split("\\.");
		// System.out.println(Arrays.toString(splitStr));
		int totalFinalLength = splitStr[0].length() + precision;
		String tempStr = String.join("", splitStr);
		String finalString = "";
		if (tempStr.length() > totalFinalLength) {
			// System.out.println("if");
			finalString = tempStr.substring(0, totalFinalLength);
		} else {
			// System.out.println("else");
			int zeroCount = totalFinalLength - tempStr.length();
			char[] chars = new char[zeroCount];
			Arrays.fill(chars, '0');
			// String text = new String(chars);
			finalString = tempStr + new String(chars);
		}
		return finalString;

	}

	@Override
	public OrderBook getOrderbook(String symbol, String limit) {

		OrderBookNaiveImpl bookNaiveImpl = allorderbooks.get(symbol);
		if (bookNaiveImpl != null) {
			return viewOrderBook(bookNaiveImpl, limit);
		}

		return null;

	}

	public BigDecimal LongToDecimalString(long decimalString, int precision) {

		String finalString = "";
		char[] chars = new char[precision];
		Arrays.fill(chars, '0');
		String dividedby = "1" + new String(chars);
		// finalString = String.valueOf(decimalString / Double.parseDouble(dividedby));
		return BigDecimal.valueOf(decimalString / Double.parseDouble(dividedby));
	}

	List<UserTrade> createOffer(String price, String size, long orderid, String action, String symbol,
			OrderBookNaiveImpl bookNaiveImpl, int userTag, boolean backup) {

		String precs = env.getProperty("precs");
		String amtprecs = env.getProperty("amtprecs");
		System.out.println("Offer Request for symbol.." + symbol);
		// Below Code Block for incremental Snapshot
		OrderBookChanges bookChanges = new OrderBookChanges();

		synchronized (this) {

			if (backup != true) {
				OrderBook orderbook = getOrderbook(symbol, null);

				if (action.equalsIgnoreCase("ASK")) {
					List<Ask> asks = orderbook.getAsk();
					double checkprice = Double.parseDouble(price);
					Optional<Double> optionalask = asks.stream().map(p -> Double.parseDouble(p.getPrice()))
							.filter(p -> p == checkprice).findAny();
					if (optionalask.isPresent()) {
						Optional<String> amount = asks.stream().filter(
								o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
								.map(o -> o.getAmount()).findAny();
						bookChanges.setPrice(price);
						bookChanges.setQuantity(
								BigDecimal.valueOf((Double.parseDouble(amount.get()) + Double.parseDouble(size)))
										.toPlainString());
						bookChanges.setSide("ASK");
						bookChanges.setSymbol(symbol);
						bookChanges.setAction("CHANGE");

					} else {
						bookChanges.setPrice(price);
						bookChanges.setQuantity(size);
						bookChanges.setSide("ASK");
						bookChanges.setSymbol(symbol);
						bookChanges.setAction("NEW");
					}
				} else {
					List<Bid> bids = orderbook.getBid();
					double checkprice = Double.parseDouble(price);
					Optional<Double> optionalbid = bids.stream().map(p -> Double.parseDouble(p.getPrice()))
							.filter(p -> p == checkprice).findAny();
					if (optionalbid.isPresent()) {
						Optional<String> amount = bids.stream().filter(
								o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
								.map(o -> o.getAmount()).findAny();
						bookChanges.setPrice(price);
						bookChanges.setQuantity(
								BigDecimal.valueOf((Double.parseDouble(amount.get()) + Double.parseDouble(size)))
										.toPlainString());
						bookChanges.setSide("BID");
						bookChanges.setSymbol(symbol);
						bookChanges.setAction("CHANGE");

					} else {
						bookChanges.setPrice(price);
						bookChanges.setQuantity(size);
						bookChanges.setSide("BID");
						bookChanges.setSymbol(symbol);
						bookChanges.setAction("NEW");
					}
				}
			}

			OrderCommand cmd = new OrderCommand();
			cmd.command = OrderCommandType.PLACE_ORDER;
			cmd.price = Long.valueOf(decimalStringToLong(price, Integer.parseInt(precs)));
			cmd.reserveBidPrice = 500000L;
			cmd.size = Long.valueOf(decimalStringToLong(size, Integer.parseInt(amtprecs)));
			cmd.orderId = orderid;
			// cmd.timestamp = 0;
			if (action.equals("BID")) {
				cmd.action = OrderAction.BID;
			} else {
				cmd.action = OrderAction.ASK;
			}
			cmd.orderType = OrderType.GTC;
			cmd.symbol = Integer.parseInt(symbol);
			cmd.uid = 301L;
			cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
			CommandResultCode result = IOrderBook.processCommand(bookNaiveImpl, cmd);

			IEventsHandler.TradeEvent evt = sendTradeEvent(cmd);
			if (evt == null) {
				System.out.println("No Trade");
			} else {
				return tradeCalculation(evt, cmd,userTag);

			}
			UserTrade userTrade = new UserTrade();
			if (backup == true) {

				userTrade.setMessage(result.toString());
				userTrade.setDbinsert("No");
			} else {
				/*
				 * Offers offer = new Offers(); offer.setOfferid(String.valueOf(orderid));
				 * offer.setSymbol(symbol); offer.setAmount(size); offer.setPrice(price);
				 * offer.setAction(action); offerRepository.save(offer);
				 */
				Offers offer = new Offers(String.valueOf(orderid), action, size, price, symbol,userTag);
				daoimpl.insertOffers(offer);
				userTrade.setOfferid(String.valueOf(orderid));
				userTrade.setMessage(result.toString());
				userTrade.setRequestamount(size);
				userTrade.setRequestprice(price);
				userTrade.setDbinsert("Yes");
				ObjectMapper Obj = new ObjectMapper();

				try {
					if (symbol.equalsIgnoreCase("241")) {

						kafkaTemplate.send(env.getProperty("kafka.topic.btcusd"),
								Obj.writeValueAsString(Arrays.asList(bookChanges)));
						System.out.println("RabbitMq::BTCUSD::" + Obj.writeValueAsString(Arrays.asList(bookChanges)));

					} else {

						kafkaTemplate.send(env.getProperty("kafka.topic.all"),
								Obj.writeValueAsString(Arrays.asList(bookChanges)));

						System.out.println("RabbitMq::BTCUSD::" + Obj.writeValueAsString(Arrays.asList(bookChanges)));

					}

				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}

			}
			return Arrays.asList(userTrade);

		}
	}

	long getOfferId() {
		long orderid = 0L;
		synchronized (this) {
			try {
				Path path = Paths.get(filename + "offerid.txt");
				orderid = Long.valueOf(Files.readAllLines(path).get(0));
				Files.write(Paths.get(filename + "offerid.txt"), String.valueOf((orderid + 1)).getBytes());
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		return orderid;
	}

	/*
	 * void writeOfferId() {
	 * 
	 * synchronized (this) { long orderid = getOfferId(); orderid++; try {
	 * Files.write(Paths.get(filename+"offerid.txt"),
	 * String.valueOf(orderid).getBytes()); } catch (IOException e) { // TODO
	 * Auto-generated catch block e.printStackTrace(); } }
	 * 
	 * }
	 */
	long getTradeId() {
		long tradeid = 0L;
		synchronized (this) {
			try {
				Path path = Paths.get(filename + "tradeid.txt");
				tradeid = Long.valueOf(Files.readAllLines(path).get(0));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return tradeid;
	}

	void writeTradeId() {

		synchronized (this) {
			long tradeid = getTradeId();
			tradeid++;
			try {
				Files.write(Paths.get(filename + "tradeid.txt"), String.valueOf(tradeid).getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	long getSlno() {
		long slno = 0L;
		synchronized (this) {
			try {
				Path path = Paths.get(filename + "slno.txt");
				slno = Long.valueOf(Files.readAllLines(path).get(0));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return slno;
	}

	void writeSlno() {
		synchronized (this) {
			long slno = getSlno();
			slno++;
			try {
				Files.write(Paths.get(filename + "slno.txt"), String.valueOf(slno).getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	long getSlnoTradeBook() {
		long slno = 0L;
		synchronized (this) {
			try {
				Path path = Paths.get(filename + "tradebookslno.txt");
				slno = Long.valueOf(Files.readAllLines(path).get(0));
				Files.write(Paths.get(filename + "tradebookslno.txt"), String.valueOf((slno + 1)).getBytes());
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return slno;
	}

	/*
	 * void writeSlnoTradeBook() { synchronized (this) { long slno
	 * =getSlnoTradeBook(); slno++; try {
	 * Files.write(Paths.get(filename+"tradebookslno.txt"),
	 * String.valueOf(slno).getBytes()); } catch (IOException e) { // TODO
	 * Auto-generated catch block e.printStackTrace(); } }
	 * 
	 * }
	 */
	OrderBook viewOrderBook(OrderBookNaiveImpl orderBookNaiveImpl, String limit) {

		synchronized (this) {

			String precs = env.getProperty("precs");
			String amtprecs = env.getProperty("amtprecs");
			L2MarketData snapshot = orderBookNaiveImpl.getL2MarketDataSnapshot(Integer.MAX_VALUE);
			int asksize = 0;
			int bidsize = 0;
			boolean isasksize = false;
			boolean isbidsize = false;
			if (limit == null) {
				asksize = snapshot.askSize;
				bidsize = snapshot.bidSize;
			} else {
				int lmt = Integer.parseInt(limit);
				if (lmt == 0) {
					asksize = snapshot.askSize;
					bidsize = snapshot.bidSize;
				} else {
					if (lmt > snapshot.askSize) {
						asksize = snapshot.askSize;
						isasksize = true;
					}
					if (lmt > snapshot.bidSize) {
						bidsize = snapshot.bidSize;
						isbidsize = true;
					}

					if (isasksize == false) {
						asksize = lmt;
					}
					if (isbidsize == false) {
						bidsize = lmt;
					}

				}
			}

			long[] askprices = snapshot.askPrices;
			long[] askvolumes = snapshot.askVolumes;
			long[] bidprices = snapshot.bidPrices;
			long[] bidvolumes = snapshot.bidVolumes;
			List<Ask> asks = new ArrayList<>();
			List<Bid> bids = new ArrayList<>();
			OrderBook orderBook = new OrderBook();

			for (int i = 0; i < asksize; i++) {
				Ask ask = new Ask();
				BigDecimal price = LongToDecimalString(askprices[i], Integer.parseInt(precs));
				BigDecimal volume = LongToDecimalString(askvolumes[i], Integer.parseInt(amtprecs));
				ask.setPrice(price.toPlainString());
				ask.setAmount(volume.toPlainString());
				asks.add(ask);
			}

			for (int i = 0; i < bidsize; i++) {
				Bid bid = new Bid();
				BigDecimal price = LongToDecimalString(bidprices[i], Integer.parseInt(precs));
				BigDecimal volume = LongToDecimalString(bidvolumes[i], Integer.parseInt(amtprecs));
				bid.setPrice(price.toPlainString());
				bid.setAmount(volume.toPlainString());
				bids.add(bid);
			}
			orderBook.setAsk(asks);
			orderBook.setBid(bids);

			return orderBook;
		}

	}

	private IEventsHandler.TradeEvent sendTradeEvent(OrderCommand cmd) {

		final MutableBoolean takerOrderCompleted = new MutableBoolean(false);
		final MutableLong mutableLong = new MutableLong(0L);
		final List<IEventsHandler.Trade> trades = new ArrayList<>();
		final MutableReference<IEventsHandler.RejectEvent> rejectEvent = new MutableReference<>(null);
		IEventsHandler.TradeEvent event = null;

		cmd.processMatcherEvents(evt -> {

			if (evt.eventType == MatcherEventType.TRADE) {

				final IEventsHandler.Trade trade = new IEventsHandler.Trade(evt.matchedOrderId, evt.matchedOrderUid,
						evt.matchedOrderCompleted, evt.price, evt.size);

				trades.add(trade);
				mutableLong.value += evt.size;

				if (evt.activeOrderCompleted) {
					takerOrderCompleted.value = true;
				}

			} else if (evt.eventType == MatcherEventType.REJECT) {

				rejectEvent.set(new IEventsHandler.RejectEvent(cmd.symbol, evt.size, evt.price, cmd.orderId, cmd.uid,
						cmd.timestamp));
			}
		});

		if (!trades.isEmpty()) {

			final IEventsHandler.TradeEvent evt = new IEventsHandler.TradeEvent(cmd.symbol, mutableLong.value,
					cmd.orderId, cmd.uid, cmd.action, takerOrderCompleted.value, cmd.timestamp, trades);

			// eventsHandler.tradeEvent(evt);
			event = evt;
			System.out.println(evt);
		}

		if (rejectEvent.ref != null) {
			// eventsHandler.rejectEvent(rejectEvent.ref);
		}
		return event;
	}

	List<UserTrade> tradeCalculation(IEventsHandler.TradeEvent trade, OrderCommand cmd,int userTag) {

		String precs = env.getProperty("precs");
		String amtprecs = env.getProperty("amtprecs");

		// Below Code for Incremental Snapshot
		synchronized (this) {
			List<OrderBookChanges> setbchanges = new ArrayList<OrderBookChanges>();
			List<String> deletedprice = new ArrayList<String>();
			List<UserTrade> usertrades = new ArrayList<UserTrade>();
			boolean takerordercompleted = trade.takeOrderCompleted;
			long totalvolume = trade.totalVolume;
			long vol = cmd.size - totalvolume;
			String remainingvol = LongToDecimalString(vol, Integer.parseInt(amtprecs)).toPlainString();
			double volprice = 0L;
			double volume = 0L;
			long totvol = 0L;
			String tradeid = String.valueOf(getTradeId());
			writeTradeId();
			for (IEventsHandler.Trade tr : trade.trades) {
				UserTrade usertrade = new UserTrade();
				String volstring = LongToDecimalString(tr.volume, Integer.parseInt(amtprecs)).toPlainString();
				String pricestring = LongToDecimalString(tr.price, Integer.parseInt(precs)).toPlainString();
				usertrade.setMatchedofferid(String.valueOf(tr.makerOrderId));
				usertrade.setVolume(volstring);
				usertrade.setMatchedofferprice(pricestring);
				volprice = volprice + (Double.valueOf(volstring) * Double.valueOf(pricestring));
				volume = volume + Double.valueOf(volstring);
				// totvol = totvol + volume;
				usertrades.add(usertrade);
				if (tr.makerOrderCompleted == true) {
					// offerRepository.deleteById(String.valueOf(tr.makerOrderId));
					daoimpl.deleteOffer(String.valueOf(tr.makerOrderId));
					// Below Code for Incremental Snapshot
					deletedprice.add(pricestring);
				} else {
					Offers offer = daoimpl.getOffer(String.valueOf(tr.makerOrderId));
					if (offer != null) {
						long qty = Long.valueOf(decimalStringToLong(offer.getAmount(), Integer.parseInt(amtprecs)));
						long qtyout = tr.volume;
						daoimpl.offerAmountUpdate(
								LongToDecimalString((qty - qtyout), Integer.parseInt(amtprecs)).toPlainString(),
								String.valueOf(tr.makerOrderId));
					}

					/*
					 * Optional<Offers> offersoptional =
					 * offerRepository.findById(String.valueOf(tr.makerOrderId)); Offers offer =
					 * offersoptional.get(); long qty =
					 * Long.valueOf(decimalStringToLong(offer.getAmount(), 9)); long qtyout =
					 * tr.volume; offer.setAmount(LongToDecimalString((qty - qtyout),
					 * 9).toPlainString()); offerRepository.save(offer);
					 */
					// Below Code for Incremental Snapshot
					OrderBookChanges bookChanges1 = new OrderBookChanges();
					bookChanges1.setPrice(pricestring);
					deletedprice.removeIf(x -> x.equalsIgnoreCase(pricestring));
					bookChanges1.setAction("CHANGE");

					bookChanges1.setSymbol(String.valueOf(trade.getSymbol()));
					if (trade.takerAction.toString().equalsIgnoreCase("ASK")) {
						bookChanges1.setSide("BID");
						OrderBook orderbook = getOrderbook(String.valueOf(trade.getSymbol()), null);
						List<Bid> bids = orderbook.getBid();
						double checkprice = Double.parseDouble(pricestring);
						Optional<Double> optionalbid = bids.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();
						if (optionalbid.isPresent()) {
							Optional<String> amount = bids.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges1
									.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
						}

					} else {
						bookChanges1.setSide("ASK");
						OrderBook orderbook = getOrderbook(String.valueOf(trade.getSymbol()), null);
						List<Ask> asks = orderbook.getAsk();
						double checkprice = Double.parseDouble(pricestring);
						Optional<Double> optionalask = asks.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();
						if (optionalask.isPresent()) {
							Optional<String> amount = asks.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges1
									.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
						}

					}
					setbchanges.add(bookChanges1);

				}

			}

			// Below code for Incremental Snapshot
			List<String> newdeletedprice = deletedprice.stream().distinct().collect(Collectors.toList());
			if (newdeletedprice.size() > 0) {
				for (int i = 0; i < newdeletedprice.size(); i++) {
					OrderBookChanges bookChanges = new OrderBookChanges();
					bookChanges.setPrice(newdeletedprice.get(i));
					if (trade.takerAction.toString().equalsIgnoreCase("ASK")) {
						bookChanges.setSide("BID");
						OrderBook orderbook = getOrderbook(String.valueOf(trade.getSymbol()), null);
						List<Bid> bids = orderbook.getBid();
						double checkprice = Double.parseDouble(newdeletedprice.get(i));
						Optional<Double> optionalbid = bids.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();
						if (optionalbid.isPresent()) {
							Optional<String> amount = bids.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges
									.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
							bookChanges.setAction("CHANGE");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						} else {
							bookChanges.setAction("TRADEDELETE");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						}
					} else {
						bookChanges.setSide("ASK");
						OrderBook orderbook = getOrderbook(String.valueOf(trade.getSymbol()), null);
						List<Ask> asks = orderbook.getAsk();
						double checkprice = Double.parseDouble(newdeletedprice.get(i));
						Optional<Double> optionalask = asks.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();
						if (optionalask.isPresent()) {
							Optional<String> amount = asks.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges
									.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
							bookChanges.setAction("CHANGE");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						} else {
							bookChanges.setAction("TRADEDELETE");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						}

					}
					setbchanges.add(bookChanges);
				}
			}

			ObjectMapper Obj = new ObjectMapper();
			Obj.setSerializationInclusion(Include.NON_NULL);

			try {

				if (String.valueOf(trade.getSymbol()).equalsIgnoreCase("241")) {

					kafkaTemplate.send(env.getProperty("kafka.topic.btcusd"), Obj.writeValueAsString(setbchanges));
					System.out.println("RabbitMq::BTCUSD::" + Obj.writeValueAsString(setbchanges));
				} else {

					kafkaTemplate.send(env.getProperty("kafka.topic.all"), Obj.writeValueAsString(setbchanges));

				}

			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			double tradeprice = volprice / volume;
			String tradepricestring = df2.format(tradeprice);
			if (takerordercompleted == true) {
				UserTrade userTrade1 = new UserTrade();
				userTrade1.setTradeprice(BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString());
				userTrade1.setRemaningqty(remainingvol);
				userTrade1.setRequestamount(LongToDecimalString(cmd.size, Integer.parseInt(amtprecs)).toPlainString());
				userTrade1.setRequestprice(LongToDecimalString(cmd.price, Integer.parseInt(precs)).toPlainString());
				userTrade1.setMessage("Offer fulfilled.");
				userTrade1.setAction(trade.takerAction.toString());
				userTrade1.setSymbol(String.valueOf(trade.getSymbol()));
				userTrade1.setTradeid(tradeid);
				userTrade1.setStatuscode("3");
				userTrade1.setOrdertype("GTC");
				usertrades.add(userTrade1);
				LocalDateTime timestamp = LocalDateTime.now();
				String type = null;
				if (trade.takerAction.toString().equalsIgnoreCase("ASK")) {
					type = "BID";
				} else {
					type = "ASK";
				}
				TradeBook tradebook = new TradeBook(String.valueOf(trade.getSymbol()),
						LongToDecimalString(totalvolume, Integer.parseInt(amtprecs)).toPlainString(),
						BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString(), timestamp, type);
				daoimpl.insertTradeBook(tradebook);
				/*
				 * //tradebook.setSlno(String.valueOf(getSlnoTradeBook())); //
				 * writeSlnoTradeBook(); tradebook.setSymbol(String.valueOf(trade.getSymbol()));
				 * tradebook.setQuantity((LongToDecimalString(totalvolume, 9).toPlainString()));
				 * tradebook.setPrice(BigDecimal.valueOf(Double.valueOf(tradepricestring)).
				 * toPlainString()); LocalDateTime timestamp = LocalDateTime.now();
				 * tradebook.setTimestamp(timestamp); bookRepository.save(tradebook);
				 */

				Offers offers = daoimpl.getOffer(String.valueOf(trade.takerOrderId));
				if (offers != null) {
					String oldprice = offers.getPrice();
					daoimpl.deleteOffer(String.valueOf(trade.takerOrderId));
					OrderBook orderbook = getOrderbook(String.valueOf(trade.getSymbol()), null);
					OrderBookChanges bookChanges = new OrderBookChanges();
					if (trade.takerAction.toString().equalsIgnoreCase("ASK")) {
						List<Ask> asks = orderbook.getAsk();
						double checkprice = Double.parseDouble(oldprice);
						Optional<Double> deletedprice1 = asks.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();

						if (deletedprice1.isPresent()) {
							Optional<String> amount = asks.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges.setQuantity(amount.get());
							bookChanges.setPrice(oldprice);
							bookChanges.setAction("CHANGE");
							bookChanges.setSide("ASK");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));

						} else {
							bookChanges.setPrice(oldprice);
							bookChanges.setAction("TRADEDELETE");
							bookChanges.setSide("ASK");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						}
					} else {
						List<Bid> bids = orderbook.getBid();
						double checkprice = Double.parseDouble(oldprice);
						Optional<Double> deletedprice1 = bids.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();

						if (deletedprice1.isPresent()) {
							Optional<String> amount = bids.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges.setQuantity(amount.get());
							bookChanges.setPrice(oldprice);
							bookChanges.setAction("CHANGE");
							bookChanges.setSide("BID");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));

						} else {
							bookChanges.setPrice(oldprice);
							bookChanges.setAction("TRADEDELETE");
							bookChanges.setSide("BID");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						}
					}

					ObjectMapper Obj1 = new ObjectMapper();
					Obj1.setSerializationInclusion(Include.NON_NULL);
					try {
						if (String.valueOf(trade.getSymbol()).equalsIgnoreCase("241")) {

							kafkaTemplate.send(env.getProperty("kafka.topic.btcusd"),
									Obj1.writeValueAsString(Arrays.asList(bookChanges)));
							System.out.println(
									"RabbitMq::BTCUSD::" + Obj1.writeValueAsString(Arrays.asList(bookChanges)));

						} else {

							kafkaTemplate.send(env.getProperty("kafka.topic.all"),
									Obj1.writeValueAsString(Arrays.asList(bookChanges)));

						}

					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

				/*
				 * Optional<Offers> offersoptional =
				 * offerRepository.findById(String.valueOf(trade.takerOrderId)); if
				 * (offersoptional.isPresent()) {
				 * offerRepository.deleteById(String.valueOf(trade.takerOrderId)); }
				 */

				TradeBookChanges tradeBookChanges = new TradeBookChanges();
				tradeBookChanges
						.setQuantity(LongToDecimalString(totalvolume, Integer.parseInt(amtprecs)).toPlainString());
				tradeBookChanges.setTimestamp(String.valueOf(timestamp));
				tradeBookChanges.setPrice(BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString());
				tradeBookChanges.setSymbol(String.valueOf(trade.getSymbol()));
				tradeBookChanges.setSide(type);

				try {

					kafkaTemplate.send(env.getProperty("kafka.topic.trade"),
							Obj.writeValueAsString(Arrays.asList(tradeBookChanges)));

				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			else {
				UserTrade userTrade1 = new UserTrade();
				userTrade1.setTradeprice(BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString());
				userTrade1.setRemaningqty(remainingvol);
				userTrade1.setRequestamount(LongToDecimalString(cmd.size, Integer.parseInt(amtprecs)).toPlainString());
				userTrade1.setRequestprice(LongToDecimalString(cmd.price, Integer.parseInt(precs)).toPlainString());
				userTrade1.setAction(trade.takerAction.toString());
				userTrade1.setSymbol(String.valueOf(trade.getSymbol()));
				userTrade1.setOfferid(String.valueOf(cmd.orderId));
				userTrade1.setMessage("Offer partial fulfilled.");
				userTrade1.setTradeid(tradeid);
				userTrade1.setStatuscode("4");
				userTrade1.setOrdertype("GTC");
				userTrade1.setUserTag(userTag);			
				usertrades.add(userTrade1);
				LocalDateTime timestamp = LocalDateTime.now();
				String type = null;
				if (trade.takerAction.toString().equalsIgnoreCase("ASK")) {
					type = "BID";
				} else {
					type = "ASK";
				}
				TradeBook tradebook = new TradeBook(String.valueOf(trade.getSymbol()),
						LongToDecimalString(totalvolume, Integer.parseInt(amtprecs)).toPlainString(),
						BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString(), timestamp, type);
				daoimpl.insertTradeBook(tradebook);
				Offers offers = daoimpl.getOffer(String.valueOf(trade.takerOrderId));
				if (offers != null) {
					String oldprice = offers.getPrice();
					daoimpl.offerAmountUpdate(remainingvol, offers.getOfferid());
					daoimpl.offerPriceUpdate(LongToDecimalString(cmd.price, Integer.parseInt(precs)).toPlainString(),
							offers.getOfferid());
					OrderBook orderbook = getOrderbook(String.valueOf(trade.getSymbol()), null);
					OrderBookChanges bookChanges = new OrderBookChanges();
					if (trade.takerAction.toString().equalsIgnoreCase("ASK")) {
						List<Ask> asks = orderbook.getAsk();
						double checkprice = Double.parseDouble(oldprice);
						Optional<Double> deletedprice1 = asks.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();

						if (deletedprice1.isPresent()) {
							Optional<String> amount = asks.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges.setQuantity(amount.get());
							bookChanges.setPrice(oldprice);
							bookChanges.setAction("CHANGE");
							bookChanges.setSide("ASK");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));

						} else {
							bookChanges.setPrice(oldprice);
							bookChanges.setAction("TRADEDELETE");
							bookChanges.setSide("ASK");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						}
					} else {
						List<Bid> bids = orderbook.getBid();
						double checkprice = Double.parseDouble(oldprice);
						Optional<Double> deletedprice1 = bids.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();

						if (deletedprice1.isPresent()) {
							Optional<String> amount = bids.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges.setQuantity(amount.get());
							bookChanges.setPrice(oldprice);
							bookChanges.setAction("CHANGE");
							bookChanges.setSide("BID");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));

						} else {
							bookChanges.setPrice(oldprice);
							bookChanges.setAction("TRADEDELETE");
							bookChanges.setSide("BID");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						}
					}

					ObjectMapper Obj1 = new ObjectMapper();
					Obj1.setSerializationInclusion(Include.NON_NULL);
					try {
						if (String.valueOf(trade.getSymbol()).equalsIgnoreCase("241")) {

							kafkaTemplate.send(env.getProperty("kafka.topic.btcusd"),
									Obj1.writeValueAsString(Arrays.asList(bookChanges)));
							System.out.println(
									"RabbitMq::BTCUSD::" + Obj1.writeValueAsString(Arrays.asList(bookChanges)));
						} else {

							kafkaTemplate.send(env.getProperty("kafka.topic.all"),
									Obj1.writeValueAsString(Arrays.asList(bookChanges)));

						}

					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				/*
				 * //tradebook.setSlno(String.valueOf(getSlnoTradeBook())); //
				 * writeSlnoTradeBook(); tradebook.setSymbol(String.valueOf(trade.getSymbol()));
				 * tradebook.setQuantity((LongToDecimalString(totalvolume, 9).toPlainString()));
				 * tradebook.setPrice(BigDecimal.valueOf(Double.valueOf(tradepricestring)).
				 * toPlainString()); tradebook.setTimestamp(timestamp);
				 * bookRepository.save(tradebook);
				 */

				TradeBookChanges tradeBookChanges = new TradeBookChanges();
				tradeBookChanges
						.setQuantity(LongToDecimalString(totalvolume, Integer.parseInt(amtprecs)).toPlainString());
				tradeBookChanges.setTimestamp(String.valueOf(timestamp));
				tradeBookChanges.setPrice(BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString());
				tradeBookChanges.setSymbol(String.valueOf(trade.getSymbol()));
				tradeBookChanges.setSide(type);

				try {

					kafkaTemplate.send(env.getProperty("kafka.topic.trade"),
							Obj.writeValueAsString(Arrays.asList(tradeBookChanges)));

				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

			return usertrades;
		}

	}

	List<UserTrade> offerTradeCalculation(List<UserTrade> result) {
		List<UserTrade> usertrades = new ArrayList<UserTrade>();

		if (result.size() > 1) {
			// Trades tr = new Trades();

			if ((result.get(result.size() - 1).getMessage()).equalsIgnoreCase("Offer partial fulfilled.")) {
				/*
				 * Offers offer = new Offers(); offer.setOfferid(result.get(result.size() -
				 * 1).getOfferid()); offer.setSymbol(result.get(result.size() - 1).getSymbol());
				 * offer.setAmount(result.get(result.size() - 1).getRemaningqty());
				 * offer.setPrice(result.get(result.size() - 1).getRequestprice());
				 * offer.setAction(result.get(result.size() - 1).getAction());
				 * 
				 * offerRepository.save(offer);
				 */
				Offers exists = daoimpl.getOffer(result.get(result.size() - 1).getOfferid());
				if (exists == null) {
					Offers offer = new Offers(result.get(result.size() - 1).getOfferid(),
							result.get(result.size() - 1).getAction(), result.get(result.size() - 1).getRemaningqty(),
							result.get(result.size() - 1).getRequestprice(), result.get(result.size() - 1).getSymbol(),result.get(result.size() - 1).getUserTag());
					daoimpl.insertOffers(offer);

				}
				// code for incremental snapshot
				OrderBookChanges bookChanges = new OrderBookChanges();
				bookChanges.setAction("NEW");
				bookChanges.setSymbol(result.get(result.size() - 1).getSymbol());
				bookChanges.setSide(result.get(result.size() - 1).getAction());
				bookChanges.setQuantity(result.get(result.size() - 1).getRemaningqty());
				bookChanges.setPrice(result.get(result.size() - 1).getRequestprice());
				ObjectMapper Obj = new ObjectMapper();
				try {

					if (result.get(result.size() - 1).getSymbol().equalsIgnoreCase("241")) {

						kafkaTemplate.send(env.getProperty("kafka.topic.btcusd"),
								Obj.writeValueAsString(Arrays.asList(bookChanges)));
						System.out.println("RabbitMq::BTCUSD::" + Obj.writeValueAsString(Arrays.asList(bookChanges)));

					} else {

						kafkaTemplate.send(env.getProperty("kafka.topic.all"),
								Obj.writeValueAsString(Arrays.asList(bookChanges)));

					}

				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			for (UserTrade usertr : result) {
				if (usertr.getMatchedofferid() != null && usertr.getVolume() != null) {

					Trades trades = new Trades(result.get(result.size() - 1).getAction(), usertr.getMatchedofferid(),
							usertr.getMatchedofferprice(), result.get(result.size() - 1).getSymbol(),
							result.get(result.size() - 1).getTradeid(), result.get(result.size() - 1).getTradeprice(),
							usertr.getVolume(), result.get(result.size() - 1).getRequestamount(),
							result.get(result.size() - 1).getRequestprice(),
							result.get(result.size() - 1).getRemaningqty(),
							result.get(result.size() - 1).getOrdertype());
					daoimpl.insertTrades(trades);
					/*
					 * String slno = String.valueOf(getSlno()); writeSlno(); tr.setSlno(slno);
					 * tr.setTradeid(result.get(result.size() - 1).getTradeid());
					 * tr.setTradeprice(result.get(result.size() - 1).getTradeprice());
					 * tr.setAction(result.get(result.size() - 1).getAction());
					 * tr.setSymbol(result.get(result.size() - 1).getSymbol());
					 * tr.setMatchedofferid(usertr.getMatchedofferid());
					 * tr.setVolume(usertr.getVolume());
					 * tr.setMatchedofferprice(usertr.getMatchedofferprice());
					 * tr.setUpdatedtime(LocalDateTime.now()); tradeRepository.save(tr);
					 */

				}
			}

			return result;
		} else {
			if (result.get(0).getMessage() == "SUCCESS") {
				UserTrade tr = new UserTrade();
				tr.setMessage("Offer created successfully.");
				tr.setSymbol(result.get(0).getSymbol());
				tr.setOfferid(result.get(0).getOfferid());
				tr.setStatuscode("1");
				tr.setRequestamount(result.get(0).getRequestamount());
				tr.setRequestprice(result.get(0).getRequestprice());
				usertrades.add(tr);

			} else if (result.get(0).getMessage() == "UPDATE") {
				UserTrade tr = new UserTrade();
				tr.setMessage("Offer price updated successfully.");
				tr.setStatuscode("2");
				usertrades.add(tr);
			}

			else if (result.get(0).getMessage() == "Offer does not exist.") {
				UserTrade tr = new UserTrade();
				tr.setMessage("Offer does not exist.");
				tr.setStatuscode("6");
				usertrades.add(tr);
			} else if (result.get(0).getMessage() == "Updated price must be different.") {
				UserTrade tr = new UserTrade();
				tr.setMessage("Updated price must be different.");
				tr.setStatuscode("0");
				usertrades.add(tr);
			} else {
				UserTrade tr = new UserTrade();
				tr.setMessage("Error");
				tr.setStatuscode("0");
				usertrades.add(tr);
			}

		}
		return usertrades;

	}

	List<UserTrade> offerModify(Model model, OrderBookNaiveImpl bookNaiveImpl) {

		String precs = env.getProperty("precs");
		String amtprecs = env.getProperty("amtprecs");
		// Below Code for Incremental Snapshot
		UserTrade usertrade = new UserTrade();
		Offers offer1 = daoimpl.getOffer(model.getOfferid());

		if (offer1 == null) {
			usertrade.setMessage("Offer does not exist.");
			usertrade.setStatuscode("6");
			return Arrays.asList(usertrade);
		}

		String oldpice = offer1.getPrice();
		String newprice = model.getPrice();

		if (Double.parseDouble(oldpice) == Double.parseDouble(newprice)) {
			usertrade.setMessage("Updated price must be different.");
			usertrade.setStatuscode("0");
			return Arrays.asList(usertrade);

		}

		boolean isnewprice = false;
		List<OrderBookChanges> orderBookChangescol = new ArrayList<OrderBookChanges>();
		OrderBookChanges bookChanges = new OrderBookChanges();
		OrderBook orderbook = getOrderbook(model.getSymbol(), null);
		if (model.getAction().equalsIgnoreCase("ASK")) {
			List<Ask> asks = orderbook.getAsk();
			double checkprice = Double.parseDouble(newprice);
			Optional<Double> optionalnewprice = asks.stream().map(p -> Double.parseDouble(p.getPrice()))
					.filter(p -> p == checkprice).findAny();
			if (optionalnewprice.isPresent()) {
				isnewprice = true;
			} else {
				bookChanges.setAction("NEW");
				bookChanges.setPrice(newprice);
				bookChanges.setQuantity(model.getAmount());
				bookChanges.setSide("ASK");
				bookChanges.setSymbol(model.getSymbol());
				orderBookChangescol.add(bookChanges);
			}

		} else {
			List<Bid> bids = orderbook.getBid();
			double checkprice = Double.parseDouble(newprice);
			Optional<Double> optionalnewprice = bids.stream().map(p -> Double.parseDouble(p.getPrice()))
					.filter(p -> p == checkprice).findAny();
			if (optionalnewprice.isPresent()) {
				isnewprice = true;
			} else {
				bookChanges.setAction("NEW");
				bookChanges.setPrice(newprice);
				bookChanges.setQuantity(model.getAmount());
				bookChanges.setSide("BID");
				bookChanges.setSymbol(model.getSymbol());
				orderBookChangescol.add(bookChanges);
			}

		}
		OrderCommand cmd = new OrderCommand();
		cmd.command = OrderCommandType.MOVE_ORDER;
		cmd.uid = 301L;
		cmd.orderId = Long.valueOf(model.getOfferid());
		cmd.price = Long.valueOf(decimalStringToLong(model.getPrice(), Integer.parseInt(precs)));
		cmd.size = Long.valueOf(decimalStringToLong(model.getAmount(), Integer.parseInt(amtprecs)));
		cmd.symbol = Integer.parseInt(model.getSymbol());
		cmd.reserveBidPrice = 500000L;
		if (model.getAction().equals("BID")) {
			cmd.action = OrderAction.BID;
		} else {
			cmd.action = OrderAction.ASK;
		}
		cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
		CommandResultCode result = IOrderBook.processCommand(bookNaiveImpl, cmd);

		if (result.toString() == "SUCCESS") {

			IEventsHandler.TradeEvent evt = sendTradeEvent(cmd);
			if (evt == null) {
				System.out.println("No Trade");

				daoimpl.offerPriceUpdate(model.getPrice(), model.getOfferid());
				/*
				 * Optional<Offers> offersoptional =
				 * offerRepository.findById(model.getOfferid()); Offers offer =
				 * offersoptional.get(); offer.setPrice(model.getPrice());
				 * offerRepository.save(offer);
				 */
				usertrade.setMessage("UPDATE");
				OrderBook orderbook1 = getOrderbook(model.getSymbol(), null);
				if (model.getAction().equalsIgnoreCase("ASK")) {
					List<Ask> asks = orderbook1.getAsk();
					if (isnewprice == true) {
						double checkprice = Double.parseDouble(newprice);
						Optional<String> amount = asks.stream().filter(
								o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
								.map(o -> o.getAmount()).findAny();
						OrderBookChanges bookChanges1 = new OrderBookChanges();
						bookChanges1.setAction("CHANGE");
						bookChanges1.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
						bookChanges1.setPrice(newprice);
						bookChanges1.setSide("ASK");
						bookChanges1.setSymbol(model.getSymbol());
						orderBookChangescol.add(bookChanges1);
					}
					double checkprice = Double.parseDouble(oldpice);
					Optional<Double> optionaloldprice = asks.stream().map(p -> Double.parseDouble(p.getPrice()))
							.filter(p -> p == checkprice).findAny();
					if (optionaloldprice.isPresent()) {
						Optional<String> amount = asks.stream().filter(
								o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
								.map(o -> o.getAmount()).findAny();
						OrderBookChanges bookChanges1 = new OrderBookChanges();
						bookChanges1.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
						bookChanges1.setAction("CHANGE");
						bookChanges1.setSymbol(String.valueOf(model.getSymbol()));
						bookChanges1.setPrice(oldpice);
						bookChanges1.setSide("ASK");
						orderBookChangescol.add(bookChanges1);
					} else {
						OrderBookChanges bookChanges1 = new OrderBookChanges();
						bookChanges1.setAction("DELETE");
						bookChanges1.setSymbol(String.valueOf(model.getSymbol()));
						bookChanges1.setPrice(oldpice);
						bookChanges1.setSide("ASK");
						orderBookChangescol.add(bookChanges1);
					}

				} else {
					List<Bid> bids = orderbook1.getBid();
					if (isnewprice == true) {
						double checkprice = Double.parseDouble(newprice);
						Optional<String> amount = bids.stream().filter(
								o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
								.map(o -> o.getAmount()).findAny();
						OrderBookChanges bookChanges1 = new OrderBookChanges();
						bookChanges1.setAction("CHANGE");
						bookChanges1.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
						bookChanges1.setPrice(newprice);
						bookChanges1.setSide("BID");
						bookChanges1.setSymbol(model.getSymbol());
						orderBookChangescol.add(bookChanges1);
					}
					double checkprice = Double.parseDouble(oldpice);
					Optional<Double> optionaloldprice = bids.stream().map(p -> Double.parseDouble(p.getPrice()))
							.filter(p -> p == checkprice).findAny();
					if (optionaloldprice.isPresent()) {
						Optional<String> amount = bids.stream().filter(
								o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
								.map(o -> o.getAmount()).findAny();
						OrderBookChanges bookChanges1 = new OrderBookChanges();
						bookChanges1.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
						bookChanges1.setAction("CHANGE");
						bookChanges1.setSymbol(String.valueOf(model.getSymbol()));
						bookChanges1.setPrice(oldpice);
						bookChanges1.setSide("BID");
						orderBookChangescol.add(bookChanges1);
					} else {
						OrderBookChanges bookChanges1 = new OrderBookChanges();
						bookChanges1.setAction("DELETE");
						bookChanges1.setSymbol(String.valueOf(model.getSymbol()));
						bookChanges1.setPrice(oldpice);
						bookChanges1.setSide("BID");
						orderBookChangescol.add(bookChanges1);
					}

				}
				ObjectMapper Obj = new ObjectMapper();
				Obj.setSerializationInclusion(Include.NON_NULL);
				try {

					kafkaTemplate.send(env.getProperty("kafka.topic.all"), Obj.writeValueAsString(orderBookChangescol));

				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} else {
				return tradeCalculation(evt, cmd,model.getUserTag());

			}
		}

		return Arrays.asList(usertrade);

	}

	List<UserTrade> offerReduce(Model model, OrderBookNaiveImpl bookNaiveImpl) {

		String precs = env.getProperty("precs");
		String amtprecs = env.getProperty("amtprecs");
		UserTrade user = new UserTrade();

		Offers offer1 = daoimpl.getOffer(model.getOfferid());

		if (offer1 == null) {
			user.setMessage("Offer does not exist.");
			user.setStatuscode("6");
			return Arrays.asList(user);
		}

		String existingamt = offer1.getAmount();
		String newamt = model.getAmount();
		if (Double.parseDouble(newamt) == Double.parseDouble(existingamt)) {
			user.setMessage("Updated amount must be different.");
			user.setStatuscode("0");
			return Arrays.asList(user);
		}
		OrderCommand cmd = new OrderCommand();
		cmd.command = OrderCommandType.REDUCE_ORDER;
		cmd.uid = 301L;
		cmd.orderId = Long.valueOf(model.getOfferid());
		cmd.size = Long.valueOf(decimalStringToLong(model.getAmount(), Integer.parseInt(amtprecs)));
		if (model.getAction().equals("BID")) {
			cmd.action = OrderAction.BID;
		} else {
			cmd.action = OrderAction.ASK;
		}
		cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
		CommandResultCode result = IOrderBook.processCommand(bookNaiveImpl, cmd);

		if (result.toString() == "SUCCESS") {

			user.setMessage("Offer amount updated successfully.");
			user.setStatuscode("2");

			// Optional<Offers> offersoptional =
			// offerRepository.findById(String.valueOf(model.getOfferid()));
			// Offers offer = offersoptional.get();
			/*
			 * long qty = Long.valueOf(decimalStringToLong(offer.getAmount(), 9)); long
			 * qtyreduce = Long.valueOf(decimalStringToLong(model.getAmount(), 9));
			 */
			Offers offer = daoimpl.getOffer(String.valueOf(model.getOfferid()));
			String price = offer.getPrice();
			// long qtyreduce = Long.valueOf(decimalStringToLong(model.getAmount(), 9));
			daoimpl.offerAmountUpdate(model.getAmount(), model.getOfferid());
			// offer.setAmount(LongToDecimalString(qtyreduce, 9).toPlainString());
			// offerRepository.save(offer);
			OrderBook orderbook = getOrderbook(model.getSymbol(), null);
			OrderBookChanges bookChanges = new OrderBookChanges();
			if (model.getAction().equalsIgnoreCase("ASK")) {
				List<Ask> asks = orderbook.getAsk();
				double checkprice = Double.parseDouble(price);
				Optional<String> amount = asks.stream()
						.filter(o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
						.map(o -> o.getAmount()).findAny();
				bookChanges.setAction("CHANGE");
				bookChanges.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
				bookChanges.setPrice(price);
				bookChanges.setSide("ASK");
				bookChanges.setSymbol(model.getSymbol());
			} else {
				List<Bid> bids = orderbook.getBid();
				double checkprice = Double.parseDouble(price);
				Optional<String> amount = bids.stream()
						.filter(o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
						.map(o -> o.getAmount()).findAny();
				bookChanges.setAction("CHANGE");
				bookChanges.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
				bookChanges.setPrice(price);
				bookChanges.setSide("BID");
				bookChanges.setSymbol(model.getSymbol());
			}

			ObjectMapper Obj = new ObjectMapper();
			Obj.setSerializationInclusion(Include.NON_NULL);
			try {

				kafkaTemplate.send(env.getProperty("kafka.topic.all"),
						Obj.writeValueAsString(Arrays.asList(bookChanges)));

			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else {
			user.setMessage("Error");
			user.setStatuscode("0");
		}
		return Arrays.asList(user);
	}

	List<UserTrade> offerRemove(Model model, OrderBookNaiveImpl bookNaiveImpl) {

		synchronized (this) {
			OrderCommand cmd = new OrderCommand();
			cmd.command = OrderCommandType.CANCEL_ORDER;
			cmd.uid = 301L;
			cmd.orderId = Long.valueOf(model.getOfferid());
			if (model.getAction().equals("BID")) {
				cmd.action = OrderAction.BID;
			} else {
				cmd.action = OrderAction.ASK;
			}
			cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
			CommandResultCode result = IOrderBook.processCommand(bookNaiveImpl, cmd);
			UserTrade user = new UserTrade();
			if (result.toString() == "SUCCESS") {

				user.setMessage("Offer deleted successfully.");
				user.setStatuscode("5");
				// offerRepository.deleteById(model.getOfferid());
				daoimpl.deleteOffer(model.getOfferid());
				OrderBook orderbook = getOrderbook(model.getSymbol(), null);
				OrderBookChanges bookChanges = new OrderBookChanges();
				if (model.getAction().equalsIgnoreCase("ASK")) {
					List<Ask> asks = orderbook.getAsk();
					double checkprice = Double.parseDouble(model.getPrice());
					Optional<Double> deletedprice = asks.stream().map(p -> Double.parseDouble(p.getPrice()))
							.filter(p -> p == checkprice).findAny();

					if (deletedprice.isPresent()) {
						Optional<String> amount = asks.stream().filter(
								o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
								.map(o -> o.getAmount()).findAny();
						bookChanges.setQuantity(amount.get());
						bookChanges.setPrice(model.getPrice());
						bookChanges.setAction("CHANGE");
						bookChanges.setSide("ASK");
						bookChanges.setSymbol(model.getSymbol());

					} else {
						bookChanges.setPrice(model.getPrice());
						bookChanges.setAction("DELETE");
						bookChanges.setSide("ASK");
						bookChanges.setSymbol(model.getSymbol());
					}
				} else {
					List<Bid> bids = orderbook.getBid();
					double checkprice = Double.parseDouble(model.getPrice());
					Optional<Double> deletedprice = bids.stream().map(p -> Double.parseDouble(p.getPrice()))
							.filter(p -> p == checkprice).findAny();

					if (deletedprice.isPresent()) {
						Optional<String> amount = bids.stream().filter(
								o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
								.map(o -> o.getAmount()).findAny();
						bookChanges.setQuantity(amount.get());
						bookChanges.setPrice(model.getPrice());
						bookChanges.setAction("CHANGE");
						bookChanges.setSide("BID");
						bookChanges.setSymbol(model.getSymbol());

					} else {
						bookChanges.setPrice(model.getPrice());
						bookChanges.setAction("DELETE");
						bookChanges.setSide("BID");
						bookChanges.setSymbol(model.getSymbol());
					}
				}

				ObjectMapper Obj = new ObjectMapper();
				Obj.setSerializationInclusion(Include.NON_NULL);
				try {

					if (model.getSymbol().equalsIgnoreCase("241")) {

						kafkaTemplate.send(env.getProperty("kafka.topic.btcusd_delete"),
								Obj.writeValueAsString(Arrays.asList(bookChanges)));
						System.out.println("RAbbitMQ::BTCUSD::" + Obj.writeValueAsString(Arrays.asList(bookChanges)));

					} else {

						kafkaTemplate.send(env.getProperty("kafka.topic.all"),
								Obj.writeValueAsString(Arrays.asList(bookChanges)));

					}

				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} else if (result.toString() == "MATCHING_UNKNOWN_ORDER_ID") {
				daoimpl.deleteOffer(model.getOfferid());
				user.setMessage("Offer does not exist.");
				user.setStatuscode("6");
			} else if (result.toString() == "INVALID_SYMBOL") {
				daoimpl.deleteOffer(model.getOfferid());
				user.setOfferid(model.getOfferid());
				user.setMessage("Action is wrong.");
				user.setStatuscode("0");
			} else {
				user.setMessage("Error");
				user.setStatuscode("0");
			}
			return Arrays.asList(user);
		}

	}

	@Override
	public List<TradeBook> marketTrades(String symbol, String limit) {

		List<TradeBook> markettrades = null;
		if (limit == null) {
			markettrades = daoimpl.alltradebook(symbol, 0);
		} else {
			markettrades = daoimpl.alltradebook(symbol, Integer.parseInt(limit));
		}

		return markettrades;

		// Comparator<Trades>sorttrades=(Trades t1,Trades
		// t2)->t2.getUpdatedtime().compareTo(t1.getUpdatedtime());
		/*
		 * List<TradeBook> sortedtrades = markettrades.stream() .sorted((TradeBook t1,
		 * TradeBook t2) -> t2.getTimestamp().compareTo(t1.getTimestamp()))
		 * .collect(Collectors.toList());
		 */
		// return sortedtrades;
		/*
		 * int size = sortedtrades.size(); int endindex = 0; if (limit == null) { return
		 * sortedtrades; } else { int lmt = Integer.parseInt(limit); if (lmt == 0) {
		 * return sortedtrades; } else { if (lmt > size) { endindex = size; } else {
		 * endindex = lmt; } return sortedtrades.subList(0, endindex); }
		 * 
		 * }
		 */

	}

	@Override
	public MarketPrice getMarketPrice(String symbol, String action, Double requestamount) {

		String precs = env.getProperty("precs");
		OrderBook book = getOrderbook(symbol, null);

		if (requestamount <= 0) {
			MarketPrice marketPrice = new MarketPrice();
			marketPrice.setMessage("Order not found.");
			marketPrice.setStatuscode("0");
			return marketPrice;
		} else {
			if (action.equalsIgnoreCase("ASK")) {
				List<Bid> bids = book.getBid();
				MarketPrice marketPrice = new MarketPrice();
				boolean flag = false;
				boolean isbreak = false;
				;
				boolean isamtzero = false;
				double fillamt = 0;
				double requiredamt = 0;
				double sum_amount = 0;
				int i, j = 0;
				if (bids.size() == 0) {
					System.out.println("OrderBook Is Empty");
					marketPrice.setMessage("OrderBook is empty.");
					marketPrice.setStatuscode("0");
					return marketPrice;
				} else {
					double[] pricearray = new double[bids.size()];
					double[] amtarray = new double[bids.size()];
					double sum = 0;
					for (i = 0; i < bids.size(); i++) {
						String price = bids.get(i).getPrice();
						String amount = bids.get(i).getAmount();
						if (requestamount != fillamt) {
							requiredamt = requestamount - fillamt;
							if (Double.parseDouble(amount) >= requiredamt) {
								pricearray[i] = Double.valueOf(price);
								amtarray[i] = requiredamt;
								fillamt = fillamt + requiredamt;
								isbreak = true;
								break;
							} else {
								fillamt = fillamt + Double.parseDouble(amount);
								pricearray[i] = Double.parseDouble(price);
								amtarray[i] = Double.parseDouble(amount);
							}

						}
					}
					String str = df2.format(fillamt);
					if (requestamount != Double.parseDouble(str)) {
						System.out.println("Order Not Found");
						marketPrice.setMessage("Order not found.");
						marketPrice.setStatuscode("0");
						return marketPrice;
					} else {
						if (isbreak == true) {
							for (j = 0; j <= i; j++) {
								try {
									sum = sum + (pricearray[j] * amtarray[j]);
								} catch (IndexOutOfBoundsException e) {
									flag = true;
									System.out.println("Order Not Found");
									marketPrice.setMessage("Order not found.");
									marketPrice.setStatuscode("0");
									return marketPrice;
								}
							}
						} else {
							for (j = 0; j < amtarray.length; j++) {
								try {
									if (amtarray[j] == 0.0) {
										isamtzero = true;
										break;
									} else {
										sum = sum + (pricearray[j] * amtarray[i]);
										sum_amount = sum_amount + amtarray[j];
									}
								} catch (IndexOutOfBoundsException e) {
									flag = true;
									marketPrice.setMessage("Order not found.");
									marketPrice.setStatuscode("0");
									return marketPrice;
								}
							}
						}
						if (isbreak == true) {
							for (int k = 0; k <= i; k++) {
								sum_amount = sum_amount + amtarray[k];
							}
						}
						if (flag == false) {
							// System.out.println("Price.." + new BigDecimal(String.valueOf(sum /
							// sum_amount)).setScale(Integer.parseInt(precs),
							// BigDecimal.ROUND_HALF_DOWN));
							marketPrice.setMessage("Order successfully found.");
							marketPrice.setStatuscode("1");
							marketPrice.setPrice(new BigDecimal(String.valueOf(sum / sum_amount))
									.setScale(Integer.parseInt(precs), RoundingMode.HALF_UP).toPlainString());
							if (isbreak == true) {
								// System.out.println("Price1:" +
								// BigDecimal.valueOf(pricearray[i]).toPlainString());
								marketPrice.setPrice1(BigDecimal.valueOf(pricearray[i]).toPlainString());
							} else if (isamtzero == true) {
								// System.out.println("Price1:" + BigDecimal.valueOf(pricearray[i -
								// 1]).toPlainString());
								marketPrice.setPrice1(BigDecimal.valueOf(pricearray[i]).toPlainString());
							}
							return marketPrice;
						}

					}

				}

			} else {
				List<Ask> asks = book.getAsk();
				MarketPrice marketPrice = new MarketPrice();
				boolean flag = false;
				boolean isbreak = false;
				;
				boolean isamtzero = false;
				double fillamt = 0;
				double requiredamt = 0;
				double sum_amount = 0;
				int i, j = 0;
				if (asks.size() == 0) {
					System.out.println("OrderBook is empty.");
					marketPrice.setMessage("OrderBook is empty.");
					marketPrice.setStatuscode("0");
					return marketPrice;
				} else {
					double[] pricearray = new double[asks.size()];
					double[] amtarray = new double[asks.size()];
					double sum = 0;
					for (i = 0; i < asks.size(); i++) {
						String price = asks.get(i).getPrice();
						String amount = asks.get(i).getAmount();
						if (requestamount != fillamt) {
							requiredamt = requestamount - fillamt;
							if (Double.parseDouble(amount) >= requiredamt) {
								pricearray[i] = Double.valueOf(price);
								amtarray[i] = requiredamt;
								fillamt = fillamt + requiredamt;
								isbreak = true;
								break;
							} else {
								fillamt = fillamt + Double.parseDouble(amount);
								pricearray[i] = Double.parseDouble(price);
								amtarray[i] = Double.parseDouble(amount);
							}

						}
					}
					String str = df2.format(fillamt);
					if (requestamount != Double.parseDouble(str)) {
						System.out.println("Order not found");
						marketPrice.setMessage("Order not found.");
						marketPrice.setStatuscode("0");
						return marketPrice;
					} else {
						if (isbreak == true) {
							for (j = 0; j <= i; j++) {
								try {
									sum = sum + (pricearray[j] * amtarray[j]);
								} catch (IndexOutOfBoundsException e) {
									flag = true;
									System.out.println("Order not found.");
									marketPrice.setMessage("Order not Found.");
									marketPrice.setStatuscode("0");
									return marketPrice;
								}
							}
						} else {
							for (j = 0; j < amtarray.length; j++) {
								try {
									if (amtarray[j] == 0.0) {
										isamtzero = true;
										break;
									} else {
										sum = sum + (pricearray[j] * amtarray[i]);
										sum_amount = sum_amount + amtarray[j];
									}
								} catch (IndexOutOfBoundsException e) {
									flag = true;
									marketPrice.setMessage("Order not found.");
									marketPrice.setStatuscode("0");
									return marketPrice;
								}
							}
						}
						if (isbreak == true) {
							for (int k = 0; k <= i; k++) {
								sum_amount = sum_amount + amtarray[k];
							}
						}
						if (flag == false) {
							// System.out.println("Price.." + new BigDecimal(String.valueOf(sum /
							// sum_amount)).setScale(Integer.parseInt(precs),
							// BigDecimal.ROUND_HALF_DOWN));
							marketPrice.setMessage("Order successfully found.");
							marketPrice.setStatuscode("1");
							marketPrice.setPrice(new BigDecimal(String.valueOf(sum / sum_amount))
									.setScale(Integer.parseInt(precs), RoundingMode.HALF_UP).toPlainString());
							if (isbreak == true) {
								// System.out.println("Price1:" +
								// BigDecimal.valueOf(pricearray[i]).toPlainString());
								marketPrice.setPrice1(BigDecimal.valueOf(pricearray[i]).toPlainString());
							} else if (isamtzero == true) {
								// System.out.println("Price1:" + BigDecimal.valueOf(pricearray[i -
								// 1]).toPlainString());
								marketPrice.setPrice1(BigDecimal.valueOf(pricearray[i - 1]).toPlainString());
							}
							return marketPrice;
						}

					}

				}

			}
		}

		return null;

	}

	@Override
	public MarketAmount getMarketAmount(String symbol, String action, Double requestprice) {

		String amtprecs = env.getProperty("amtprecs");
		OrderBook book = getOrderbook(symbol, null);

		if (requestprice <= 0) {
			MarketAmount marketAmount = new MarketAmount();
			marketAmount.setMessage("Order not found.");
			marketAmount.setStatuscode("0");
			return marketAmount;
		} else {
			if (action.equalsIgnoreCase("ASK")) {
				List<Bid> bids = book.getBid();
				MarketAmount marketAmount = new MarketAmount();
				boolean flag = false;
				boolean isbreak = false;
				;
				boolean isamtzero = false;
				double fillprice = 0;
				double requiredprice = 0;
				double sum_amount = 0;
				int i, j = 0;
				if (bids.size() == 0) {
					System.out.println("OrderBook Is Empty");
					marketAmount.setMessage("OrderBook is empty.");
					marketAmount.setStatuscode("0");
					return marketAmount;
				} else {
					double[] pricearray = new double[bids.size()];
					double[] amtarray = new double[bids.size()];
					double sum = 0;
					for (i = 0; i < bids.size(); i++) {
						String price = bids.get(i).getPrice();
						String amount = bids.get(i).getAmount();
						if (requestprice != fillprice) {

							requiredprice = requiredprice - fillprice;

							if (Double.parseDouble(price) >= requiredprice) {
								pricearray[i] = requiredprice;
								amtarray[i] = Double.valueOf(amount);
								fillprice = fillprice + requiredprice;
								isbreak = true;
								break;
							} else {

								fillprice = fillprice + Double.parseDouble(price);
								pricearray[i] = Double.parseDouble(price);
								amtarray[i] = Double.parseDouble(amount);
							}

						}
					}
					String str = df2.format(fillprice);
					if (requestprice != Double.parseDouble(str)) {
						System.out.println("Order Not Found");
						marketAmount.setMessage("Order not found.");
						marketAmount.setStatuscode("0");
						return marketAmount;
					} else {
						if (isbreak == true) {
							for (j = 0; j <= i; j++) {
								try {
									sum_amount = sum_amount + amtarray[j];
								} catch (IndexOutOfBoundsException e) {
									flag = true;
									System.out.println("Order Not Found");
									marketAmount.setMessage("Order not found.");
									marketAmount.setStatuscode("0");
									return marketAmount;
								}
							}
						} else {
							for (j = 0; j < amtarray.length; j++) {
								try {
									if (amtarray[j] == 0.0) {
										isamtzero = true;
										break;
									} else {

										sum_amount = sum_amount + amtarray[j];
									}
								} catch (IndexOutOfBoundsException e) {
									flag = true;
									marketAmount.setMessage("Order not found.");
									marketAmount.setStatuscode("0");
									return marketAmount;
								}
							}
						}

						if (flag == false) {
							// System.out.println("Price.." + new BigDecimal(String.valueOf(sum /
							// sum_amount)).setScale(Integer.parseInt(precs),
							// BigDecimal.ROUND_HALF_DOWN));
							marketAmount.setMessage("Order successfully found.");
							marketAmount.setStatuscode("1");
							marketAmount.setAmount(new BigDecimal(String.valueOf(sum_amount))
									.setScale(Integer.parseInt(amtprecs), RoundingMode.HALF_UP).toPlainString());

							return marketAmount;
						}

					}

				}

			} else {
				List<Ask> asks = book.getAsk();
				MarketAmount marketAmount = new MarketAmount();
				boolean flag = false;
				boolean isbreak = false;
				;
				boolean isamtzero = false;
				double fillprice = 0;
				double requiredprice = 0;
				double sum_amount = 0;
				int i, j = 0;
				if (asks.size() == 0) {
					System.out.println("OrderBook is empty.");
					marketAmount.setMessage("OrderBook is empty.");
					marketAmount.setStatuscode("0");
					return marketAmount;
				} else {
					double[] pricearray = new double[asks.size()];
					double[] amtarray = new double[asks.size()];
					double sum = 0;
					for (i = 0; i < asks.size(); i++) {
						String price = asks.get(i).getPrice();
						String amount = asks.get(i).getAmount();
						if (requestprice != fillprice) {

							requiredprice = requestprice - fillprice;

							if (Double.parseDouble(price) >= requiredprice) {
								pricearray[i] = requiredprice;
								amtarray[i] = Double.valueOf(amount);
								fillprice = fillprice + requiredprice;
								isbreak = true;
								break;
							} else {
								fillprice = fillprice + Double.parseDouble(price);
								pricearray[i] = Double.parseDouble(price);
								amtarray[i] = Double.parseDouble(amount);
							}

						}
					}
					String str = df2.format(fillprice);
					if (requestprice != Double.parseDouble(str)) {
						System.out.println("Order not found");
						marketAmount.setMessage("Order not found.");
						marketAmount.setStatuscode("0");
						return marketAmount;
					} else {
						if (isbreak == true) {
							for (j = 0; j <= i; j++) {
								try {
									sum_amount = sum_amount + amtarray[j];

								} catch (IndexOutOfBoundsException e) {
									flag = true;
									System.out.println("Order not found.");
									marketAmount.setMessage("Order not Found.");
									marketAmount.setStatuscode("0");
									return marketAmount;
								}
							}
						} else {
							for (j = 0; j < amtarray.length; j++) {
								try {
									if (amtarray[j] == 0.0) {
										isamtzero = true;
										break;
									} else {
										sum_amount = sum_amount + amtarray[j];
									}
								} catch (IndexOutOfBoundsException e) {
									flag = true;
									marketAmount.setMessage("Order not found.");
									marketAmount.setStatuscode("0");
									return marketAmount;
								}
							}
						}

						if (flag == false) {
							// System.out.println("Price.." + new BigDecimal(String.valueOf(sum /
							// sum_amount)).setScale(Integer.parseInt(precs),
							// BigDecimal.ROUND_HALF_DOWN));
							marketAmount.setMessage("Order successfully found.");
							marketAmount.setStatuscode("1");
							marketAmount.setAmount(new BigDecimal(String.valueOf(sum_amount))
									.setScale(Integer.parseInt(amtprecs), RoundingMode.HALF_UP).toPlainString());

							return marketAmount;
						}

					}

				}

			}
		}

		return null;
	}

	List<UserTrade> createOfferIOCFOK(String price, String size, long orderid, String action, String symbol,
			OrderBookNaiveImpl bookNaiveImpl, String ordertype) {
		String precs = env.getProperty("precs");
		String amtprecs = env.getProperty("amtprecs");

		synchronized (this) {
			OrderCommand cmd = new OrderCommand();
			cmd.command = OrderCommandType.PLACE_ORDER;
			cmd.price = Long.valueOf(decimalStringToLong(price, Integer.parseInt(precs)));
			cmd.reserveBidPrice = 500000L;
			cmd.size = Long.valueOf(decimalStringToLong(size, Integer.parseInt(amtprecs)));
			cmd.orderId = orderid;
			// cmd.timestamp = 0;
			if (action.equals("BID")) {
				cmd.action = OrderAction.BID;
			} else {
				cmd.action = OrderAction.ASK;
			}
			if (ordertype.equalsIgnoreCase("IOC")) {
				cmd.orderType = OrderType.IOC;
			} else if (ordertype.equalsIgnoreCase("FOK")) {
				cmd.orderType = OrderType.FOK_BUDGET;
			}
			cmd.symbol = Integer.parseInt(symbol);
			cmd.uid = 301L;
			cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
			CommandResultCode result = IOrderBook.processCommand(bookNaiveImpl, cmd);
			IEventsHandler.TradeEvent evt = sendTradeEvent(cmd);
			if (evt == null) {
				System.out.println("No Trade");
				UserTrade tmp = new UserTrade();
				tmp.setMessage("As ordertype is IOC/FOK so trade has to be happened.");
				tmp.setStatuscode("0");
				return Arrays.asList(tmp);
			} else {
				return tradeIOCFOK(evt, cmd, ordertype);
			}
		}

	}

	List<UserTrade> tradeIOCFOK(IEventsHandler.TradeEvent trade, OrderCommand cmd, String ordertype) {
		String precs = env.getProperty("precs");
		String amtprecs = env.getProperty("amtprecs");
		List<OrderBookChanges> setbchanges = new ArrayList<OrderBookChanges>();
		List<String> deletedprice = new ArrayList<String>();
		List<UserTrade> usertrades = new ArrayList<UserTrade>();

		synchronized (this) {

			boolean takerordercompleted = trade.takeOrderCompleted;
			long totalvolume = trade.totalVolume;
			long vol = cmd.size - totalvolume;
			String remainingvol = LongToDecimalString(vol, Integer.parseInt(amtprecs)).toPlainString();
			double volprice = 0L;
			double volume = 0L;
			long totvol = 0L;
			String tradeid = String.valueOf(getTradeId());
			writeTradeId();
			for (IEventsHandler.Trade tr : trade.trades) {
				UserTrade usertrade = new UserTrade();
				String volstring = LongToDecimalString(tr.volume, Integer.parseInt(amtprecs)).toPlainString();
				String pricestring = LongToDecimalString(tr.price, Integer.parseInt(precs)).toPlainString();
				usertrade.setMatchedofferid(String.valueOf(tr.makerOrderId));
				usertrade.setVolume(volstring);
				usertrade.setMatchedofferprice(pricestring);
				volprice = volprice + (Double.valueOf(volstring) * Double.valueOf(pricestring));
				volume = volume + Double.valueOf(volstring);
				usertrades.add(usertrade);
				if (tr.makerOrderCompleted == true) {
					daoimpl.deleteOffer(String.valueOf(tr.makerOrderId));
					deletedprice.add(pricestring);
				} else {
					Offers offer = daoimpl.getOffer(String.valueOf(tr.makerOrderId));
					long qty = Long.valueOf(decimalStringToLong(offer.getAmount(), Integer.parseInt(amtprecs)));
					long qtyout = tr.volume;
					daoimpl.offerAmountUpdate(
							LongToDecimalString((qty - qtyout), Integer.parseInt(amtprecs)).toPlainString(),
							String.valueOf(tr.makerOrderId));
					OrderBookChanges bookChanges1 = new OrderBookChanges();
					bookChanges1.setPrice(pricestring);
					deletedprice.removeIf(x -> x.equalsIgnoreCase(pricestring));
					bookChanges1.setAction("CHANGE");
					bookChanges1.setSymbol(String.valueOf(trade.getSymbol()));
					if (trade.takerAction.toString().equalsIgnoreCase("ASK")) {
						bookChanges1.setSide("BID");
						OrderBook orderbook = getOrderbook(String.valueOf(trade.getSymbol()), null);
						List<Bid> bids = orderbook.getBid();
						double checkprice = Double.parseDouble(pricestring);
						Optional<Double> optionalbid = bids.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();
						if (optionalbid.isPresent()) {
							Optional<String> amount = bids.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges1
									.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
						}

					} else {
						bookChanges1.setSide("ASK");
						OrderBook orderbook = getOrderbook(String.valueOf(trade.getSymbol()), null);
						List<Ask> asks = orderbook.getAsk();
						double checkprice = Double.parseDouble(pricestring);
						Optional<Double> optionalask = asks.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();
						if (optionalask.isPresent()) {
							Optional<String> amount = asks.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges1
									.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
						}

					}
					setbchanges.add(bookChanges1);

				}

			}

			List<String> newdeletedprice = deletedprice.stream().distinct().collect(Collectors.toList());
			if (newdeletedprice.size() > 0) {
				for (int i = 0; i < newdeletedprice.size(); i++) {
					OrderBookChanges bookChanges = new OrderBookChanges();
					bookChanges.setPrice(newdeletedprice.get(i));
					if (trade.takerAction.toString().equalsIgnoreCase("ASK")) {
						bookChanges.setSide("BID");
						OrderBook orderbook = getOrderbook(String.valueOf(trade.getSymbol()), null);
						List<Bid> bids = orderbook.getBid();
						double checkprice = Double.parseDouble(newdeletedprice.get(i));
						Optional<Double> optionalbid = bids.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();
						if (optionalbid.isPresent()) {
							Optional<String> amount = bids.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges
									.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
							bookChanges.setAction("CHANGE");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						} else {
							bookChanges.setAction("DELETE");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						}
					} else {
						bookChanges.setSide("ASK");
						OrderBook orderbook = getOrderbook(String.valueOf(trade.getSymbol()), null);
						List<Ask> asks = orderbook.getAsk();
						double checkprice = Double.parseDouble(newdeletedprice.get(i));
						Optional<Double> optionalask = asks.stream().map(p -> Double.parseDouble(p.getPrice()))
								.filter(p -> p == checkprice).findAny();
						if (optionalask.isPresent()) {
							Optional<String> amount = asks.stream().filter(
									o -> o.getPrice().equalsIgnoreCase(BigDecimal.valueOf(checkprice).toPlainString()))
									.map(o -> o.getAmount()).findAny();
							bookChanges
									.setQuantity(BigDecimal.valueOf(Double.parseDouble(amount.get())).toPlainString());
							bookChanges.setAction("CHANGE");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						} else {
							bookChanges.setAction("DELETE");
							bookChanges.setSymbol(String.valueOf(trade.getSymbol()));
						}

					}
					setbchanges.add(bookChanges);
				}
			}

			ObjectMapper Obj = new ObjectMapper();
			Obj.setSerializationInclusion(Include.NON_NULL);

			try {

				kafkaTemplate.send(env.getProperty("kafka.topic.all"), Obj.writeValueAsString(setbchanges));

			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			double tradeprice = volprice / volume;
			String tradepricestring = df2.format(tradeprice);
			if (takerordercompleted == true) {
				UserTrade userTrade1 = new UserTrade();
				userTrade1.setTradeprice(BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString());
				userTrade1.setRemaningqty(remainingvol);
				userTrade1.setRequestamount(LongToDecimalString(cmd.size, Integer.parseInt(amtprecs)).toPlainString());
				userTrade1.setRequestprice(LongToDecimalString(cmd.price, Integer.parseInt(precs)).toPlainString());
				userTrade1.setMessage("Offer fulfilled.");
				userTrade1.setAction(trade.takerAction.toString());
				userTrade1.setSymbol(String.valueOf(trade.getSymbol()));
				userTrade1.setTradeid(tradeid);
				userTrade1.setStatuscode("3");
				userTrade1.setOrdertype(ordertype);
				usertrades.add(userTrade1);
				LocalDateTime timestamp = LocalDateTime.now();
				String type = null;
				if (trade.takerAction.toString().equalsIgnoreCase("ASK")) {
					type = "BID";
				} else {
					type = "ASK";
				}
				TradeBook tradebook = new TradeBook(String.valueOf(trade.getSymbol()),
						LongToDecimalString(totalvolume, Integer.parseInt(amtprecs)).toPlainString(),
						BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString(), timestamp, type);
				daoimpl.insertTradeBook(tradebook);
				TradeBookChanges tradeBookChanges = new TradeBookChanges();
				tradeBookChanges
						.setQuantity(LongToDecimalString(totalvolume, Integer.parseInt(amtprecs)).toPlainString());
				tradeBookChanges.setTimestamp(String.valueOf(timestamp));
				tradeBookChanges.setPrice(BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString());
				tradeBookChanges.setSymbol(String.valueOf(trade.getSymbol()));
				tradeBookChanges.setSide(type);

				try {

					kafkaTemplate.send("kafka.topic.trade", Obj.writeValueAsString(Arrays.asList(tradeBookChanges)));

				} catch (AmqpException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			else {
				UserTrade userTrade1 = new UserTrade();
				userTrade1.setTradeprice(BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString());
				userTrade1.setRemaningqty(remainingvol);
				userTrade1.setRequestamount(LongToDecimalString(cmd.size, Integer.parseInt(amtprecs)).toPlainString());
				userTrade1.setRequestprice(LongToDecimalString(cmd.price, Integer.parseInt(precs)).toPlainString());
				userTrade1.setAction(trade.takerAction.toString());
				userTrade1.setSymbol(String.valueOf(trade.getSymbol()));
				// userTrade1.setOfferid(String.valueOf(cmd.orderId));
				userTrade1.setMessage("Offer partial fulfilled.");
				userTrade1.setTradeid(tradeid);
				userTrade1.setStatuscode("4");
				userTrade1.setRemarks("IOC Order type, so " + remainingvol + " quantity offer have been canceled.");
				userTrade1.setOrdertype(ordertype);
				usertrades.add(userTrade1);
				LocalDateTime timestamp = LocalDateTime.now();
				String type = null;
				if (trade.takerAction.toString().equalsIgnoreCase("ASK")) {
					type = "BID";
				} else {
					type = "ASK";
				}
				TradeBook tradebook = new TradeBook(String.valueOf(trade.getSymbol()),
						LongToDecimalString(totalvolume, Integer.parseInt(amtprecs)).toPlainString(),
						BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString(), timestamp, type);
				daoimpl.insertTradeBook(tradebook);
				TradeBookChanges tradeBookChanges = new TradeBookChanges();
				tradeBookChanges
						.setQuantity(LongToDecimalString(totalvolume, Integer.parseInt(amtprecs)).toPlainString());
				tradeBookChanges.setTimestamp(String.valueOf(timestamp));
				tradeBookChanges.setPrice(BigDecimal.valueOf(Double.valueOf(tradepricestring)).toPlainString());
				tradeBookChanges.setSymbol(String.valueOf(trade.getSymbol()));
				tradeBookChanges.setSide(type);

				try {

					kafkaTemplate.send(env.getProperty("kafka.topic.trade"),
							Obj.writeValueAsString(Arrays.asList(tradeBookChanges)));

				} catch (AmqpException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			for (UserTrade usertr : usertrades) {
				if (usertr.getMatchedofferid() != null && usertr.getVolume() != null) {

					Trades trades = new Trades(usertrades.get(usertrades.size() - 1).getAction(),
							usertr.getMatchedofferid(), usertr.getMatchedofferprice(),
							usertrades.get(usertrades.size() - 1).getSymbol(),
							usertrades.get(usertrades.size() - 1).getTradeid(),
							usertrades.get(usertrades.size() - 1).getTradeprice(), usertr.getVolume(),
							usertrades.get(usertrades.size() - 1).getRequestamount(),
							usertrades.get(usertrades.size() - 1).getRequestprice(),
							usertrades.get(usertrades.size() - 1).getRemaningqty(),
							usertrades.get(usertrades.size() - 1).getOrdertype());
					daoimpl.insertTrades(trades);

				}
			}

			return usertrades;

		}

	}

	@Override
	public UserTrade checkOrderStatus(String symbol, String offerid) {
		UserTrade res = new UserTrade();
		OrderBookNaiveImpl bookNaiveImpl = allorderbooks.get(symbol);
		CommandResultCode result = bookNaiveImpl.checkOrder(Long.valueOf(offerid));
		if (result.toString() == "SUCCESS") {
			Offers offer = daoimpl.getOffer(offerid);
			if (offer == null) {
				System.out.println("Offerid does not exist in database.");
				res.setMessage("OfferID does exist");
				res.setStatuscode("1");
			} else {
				res.setMessage("OfferID does exist");
				res.setStatuscode("1");
			}

		} else {
			res.setMessage("OfferID does not exist");
			res.setStatuscode("0");
		}
		return res;
	}

	@Override
	public UserTrade addAsset(Model model) {
		UserTrade res = new UserTrade();
		boolean flag_char = checkCharacters(model.getAssetcode());
		if (flag_char == false) {
			boolean flag = daoimpl.checkAsset(Integer.parseInt(model.getSymbol()), model.getAssetcode());

			if (flag == false) {
				int r = daoimpl.insertAsset(model);
				if (r > 0) {
					OrderBookNaiveImpl orderBook = new OrderBookNaiveImpl(
							getCoreSymbolSpec(Integer.valueOf(model.getSymbol())), LoggingConfiguration.DEFAULT);
					allorderbooks.put(model.getSymbol(), orderBook);
					res.setMessage("Asset Created Successfully!!");
					res.setStatuscode("1");
				} else {
					res.setMessage("Error");
					res.setStatuscode("0");
				}
			} else {
				res.setMessage("Duplicate asset can not be allowed.");
				res.setStatuscode("0");
			}

		} else {

			res.setMessage("Please remove specical character(s) from asset code.");
			res.setStatuscode("0");
		}

		return res;
	}

	boolean checkCharacters(String symbol) {
		Pattern pattern = Pattern.compile("[^a-zA-Z0-9]");
		Matcher matcher = pattern.matcher(symbol);
		boolean flag = matcher.find();
		if (flag == true) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public UserTrade deleteAsset(Model model) {
		UserTrade res = new UserTrade();
		boolean flag = daoimpl.checkOfferBySymbol(model.getSymbol());
		if (flag == false) {
			int r = daoimpl.deleteAsset(model);
			if (r > 0) {
				allorderbooks.remove(model.getSymbol());
				res.setMessage("Asset deleted successfully");
				res.setStatuscode("1");
			} else {
				res.setMessage("Error.");
				res.setStatuscode("0");
			}
		} else {
			res.setMessage("Symbol can not be deleted because it has offer(s).");
			res.setStatuscode("0");
		}
		return res;
	}

}
