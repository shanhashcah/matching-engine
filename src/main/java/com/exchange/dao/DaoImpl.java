package com.exchange.dao;

import java.util.List;

import org.apache.kafka.common.protocol.types.Field.Str;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.exchange.model.Model;
import com.exchange.model.Offers;
import com.exchange.model.Symbols;
import com.exchange.model.TradeBook;
import com.exchange.model.Trades;

@Component
public class DaoImpl implements IDao {

	@Autowired
	JdbcTemplate template;

	@Autowired
	Environment env;

	@Override
	public List<Offers> allOffers() {
		String offerstable = env.getProperty("offerstable");
		List<Offers> alloffer = template.query("select offerid,action,amount,price,symbol,user_tag from " + offerstable,
				(result, rownum) -> new Offers(result.getString("offerid"), result.getString("action"),
						result.getString("amount"), result.getString("price"), result.getString("symbol"),result.getInt("user_tag")));
		return alloffer;
	}

	@Override
	public int insertOffers(Offers o) {
		String offerstable = env.getProperty("offerstable");
		String query = "INSERT INTO " + offerstable + " (offerid,action,amount,price,symbol,user_tag) VALUES(?,?,?,?,?,?)";
		return template.update(query, o.getOfferid(), o.getAction(), o.getAmount(), o.getPrice(), o.getSymbol(),o.getUserTag());
	}

	@Override
	public int deleteOffer(String offerid) {
		String offerstable = env.getProperty("offerstable");
		String query = "DELETE FROM " + offerstable + " WHERE offerid =?";
		return template.update(query, offerid);

	}

	@Override
	public Offers getOffer(String offerid) {
		String offerstable = env.getProperty("offerstable");
		String query = "select * from " + offerstable + " where offerid=?";
		Offers offer = null;
		try {
			offer = template.queryForObject(query, new BeanPropertyRowMapper<>(Offers.class), offerid);

		} catch (EmptyResultDataAccessException e) {
			return null;
		}

		return offer;
	}

	@Override
	public int offerAmountUpdate(String amount, String offerid) {
		String offerstable = env.getProperty("offerstable");
		String query = "update " + offerstable + " set amount=? where offerid=?";
		return template.update(query, amount, offerid);
	}

	@Override
	public int insertTradeBook(TradeBook book) {
		String tradebooktable = env.getProperty("tradebooktable");
		String query = "insert into " + tradebooktable + "(symbol,quantity,price,timestamp,side) values(?,?,?,?,?)";
		return template.update(query, book.getSymbol(), book.getQuantity(), book.getPrice(), book.getTimestamp(),
				book.getSide());
	}

	@Override
	public int insertTrades(Trades trades) {
		String tradestable = env.getProperty("tradestable");
		String query = "insert into " + tradestable
				+ "(taker_action,matchedofferid,matchedofferprice,symbol,tradeid, tradeprice,volume,request_amount,request_price,remaning_qty,ordertype) values(?,?,?,?,?,?,?,?,?,?,?)";
		return template.update(query, trades.getAction(), trades.getMatchedofferid(), trades.getMatchedofferprice(),
				trades.getSymbol(), trades.getTradeid(), trades.getTradeprice(), trades.getVolume(),
				trades.getRequestamount(), trades.getRequestprice(), trades.getRemaningqty(), trades.getOrdertype());
	}

	@Override
	public int offerPriceUpdate(String price, String offerid) {

		String offerstable = env.getProperty("offerstable");
		String query = "update " + offerstable + " set price=? where offerid=?";
		return template.update(query, price, offerid);
	}

	@Override
	public List<TradeBook> alltradebook(String symbol, int limit) {
		String tradebooktable = env.getProperty("tradebooktable");
		String query = null;
		if (limit == 0) {
			query = "select quantity,price,timestamp,side from " + tradebooktable + " where symbol='" + symbol
					+ "' order by timestamp desc";
		} else {
			query = "select quantity,price,timestamp,side from " + tradebooktable + " where symbol='" + symbol
					+ "' order by timestamp desc  limit " + limit;
		}

		List<TradeBook> alltradebook = template.query(query, new BeanPropertyRowMapper<>(TradeBook.class));
		return alltradebook;

	}

	@Override
	public String activeOffers() {
		String offerstable = env.getProperty("offerstable");
		String query = "select count(*) from " + offerstable;
		return template.queryForObject(query, String.class);
	}

	@Override
	public List<Symbols> getSymbol() {

		String symbolstable = env.getProperty("symbolstable");

		String projtype = env.getProperty("project.type");
		String query = null;
		List<Symbols> allsymbols = null;
		if (projtype.equalsIgnoreCase("spot")) {
			System.out.println("Project type is Spot");
			query = "select symbol from " + symbolstable + " where is_active=1";
			allsymbols = template.query(query, new BeanPropertyRowMapper<>(Symbols.class));
		} else if (projtype.equalsIgnoreCase("future")) {
			System.out.println("Project type is Future");
			query = "select symbol from " + symbolstable + " where pair_active=1";
			allsymbols = template.query(query, new BeanPropertyRowMapper<>(Symbols.class));
		}

		return allsymbols;

	}

	@Override
	public int insertAsset(Model model) {
		String query = "INSERT INTO " + env.getProperty("symbolstable") + " (symbol,pair_name) VALUES(?,?)";
		return template.update(query, Integer.valueOf(model.getSymbol()), model.getAssetcode().toUpperCase());
	}

	@Override
	public boolean checkAsset(int symbol, String pair_name) {
		String query = "select count(*) from symbols where symbol=" + symbol + " or pair_name='" + pair_name + "'";
		int row = template.queryForObject(query, Integer.class);
		if (row > 0) {
			return true;
		} else {

			return false;
		}

	}
	

	@Override
	public int deleteAsset(Model model) {
		String query="delete from symbols where symbol=?";
		return template.update(query,Integer.parseInt(model.getSymbol()));
	}

	@Override
	public boolean checkOfferBySymbol(String symbol) {
		
		String query="select count(*) from offers where symbol='"+symbol+"'";
		int row = template.queryForObject(query, Integer.class);
		if (row > 0) {
			return true;
		} else {

			return false;
		}
	}

}
