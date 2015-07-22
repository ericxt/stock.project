package com.rongdata.dbUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

enum Quotation {
	TradingTime, ContractId, ExchangeId, PreSettlementPrice, CurrSettlementPrice, AveragePrice, PreClosePrice, CurrClosePrice, CurrOpenPrice, PreHoldings, Holdings, LatestPrice, Volume, TurnOver, TopQuotation, BottomQuotation, TopPrice, BottomPrice, PreDelta, CurrDelta, BidPrice1, AskPrice1, BidVolume1, AskVolume1, BidPrice2, AskPrice2, BidVolume2, AskVolume2, BidPrice3, AskPrice3, BidVolume3, AskVolume3, BidPrice4, AskPrice4, BidVolume4, AskVolume4, BidPrice5, AskPrice5, BidVolume5, AskVolume5;
}

public class RawDataAccess {
	private Connection conn;

	public RawDataAccess() {
		// TODO Auto-generated constructor stub
	}

	public RawDataAccess(Connection conn) {
		this.conn = conn;
	}

	public ResultSet getRawData(String sql) {
		// sql = "select * from xcube.market_quotation limit 10";
		if (conn == null) {
			System.out.println("rawdataaccess.getrawdata ---> get conn");
			conn = MysqlDBUtil.getConnection();
		}
		PreparedStatement prepareStatement = null;
		try {
			prepareStatement = conn.prepareStatement(sql);
			ResultSet resultSet = prepareStatement.executeQuery();
			return resultSet;

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	// extract settlement price from settlePrice table
	public BigDecimal getNDaysBeforePrice(String ticker, Date datetime, int daysCount) {
		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		String sql = "select preSettlePrice from xcube.settlement_data where ticker = '"
				+ ticker
				+ "' and tradingDate = subdate('"
				+ datetime
				+ "',"
				+ daysCount + ");";

		System.out.println("RawDataAccess.getNdaysbeforeprice >>> " + sql);

		PreparedStatement prestmt = null;
		try {
			prestmt = conn.prepareStatement(sql);
			ResultSet rtst = prestmt.executeQuery();
			while (rtst.next()) {
				BigDecimal settlePrice = rtst.getBigDecimal("preSettlePrice");
				System.out.println(settlePrice);
				return settlePrice;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return BigDecimal.ZERO;
	}

	public BigInteger getPrevCumVolume(String ticker, Timestamp datetime,
			String tickerType) {
		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		String futuresSql = "select volume from xcube.futures_quotation where contractid = '"
				+ ticker
				+ "' order by tradingtime >= '"
				+ datetime
				+ "', tradingtime desc limit 1;";
		String debtSql = "select volume from xcube.debt_quotation where contractid = '"
				+ ticker
				+ "' order by tradingtime >= '"
				+ datetime
				+ "', tradingtime desc limit 1;";
		String indexSql = "select volume from xcube.index_quotation where contractid = '"
				+ ticker
				+ "' order by tradingtime >= '"
				+ datetime
				+ "', tradingtime desc limit 1;";
		String stockSql = "select volume from xcube.stock_quotation where contractid = '"
				+ ticker
				+ "' order by tradingtime >= '"
				+ datetime
				+ "', tradingtime desc limit 1;";
		PreparedStatement prestmt = null;
		try {
			if (tickerType == "futures") {
				prestmt = conn.prepareStatement(futuresSql);
			} else if (tickerType == "debt") {
				prestmt = conn.prepareStatement(debtSql);
			} else if (tickerType == "index") {
				prestmt = conn.prepareStatement(indexSql);
			} else if (tickerType == "stock") {
				prestmt = conn.prepareStatement(stockSql);
			}

			ResultSet rest = prestmt.executeQuery();
			BigInteger preCumVolume = BigInteger.ZERO;
			while (rest.next()) {
				preCumVolume = BigInteger.valueOf(rest.getLong("Volume"));
			}
			return preCumVolume;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public BigInteger getPrevHoldings(String ticker, Timestamp datetime,
			String tickerType) {
		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		String futuresSql = "select holdings from xcube.futures_quotation where contractid = '"
				+ ticker
				+ "' order by tradingtime >= '"
				+ datetime
				+ "', tradingtime desc limit 1;";
		String debtSql = "select holdings from xcube.debt_quotation where contractid = '"
				+ ticker
				+ "' order by tradingtime >= '"
				+ datetime
				+ "', tradingtime desc limit 1;";
		String indexSql = "select holdings from xcube.index_quotation where contractid = '"
				+ ticker
				+ "' order by tradingtime >= '"
				+ datetime
				+ "', tradingtime desc limit 1;";
		String stockSql = "select holdings from xcube.stock_quotation where contractid = '"
				+ ticker
				+ "' order by tradingtime >= '"
				+ datetime
				+ "', tradingtime desc limit 1;";

		PreparedStatement prestmt = null;
		try {
			if (tickerType == "futures") {
				prestmt = conn.prepareStatement(futuresSql);
			} else if (tickerType == "debt") {
				prestmt = conn.prepareStatement(debtSql);
			} else if (tickerType == "index") {
				prestmt = conn.prepareStatement(indexSql);
			} else if (tickerType == "stock") {
				prestmt = conn.prepareStatement(stockSql);
			}

			ResultSet rest = prestmt.executeQuery();
			BigInteger prevHoldings = BigInteger.ZERO;
			while (rest.next()) {
				prevHoldings = BigInteger.valueOf(rest.getLong("Holdings"));
			}
			return prevHoldings;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public ResultSet getStockComment(String sql) {
		// TODO Auto-generated method stub
		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		try {
			PreparedStatement prestmt = conn.prepareStatement(sql);
			ResultSet resultSet = prestmt.executeQuery();
			return resultSet;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public ResultSet getTickerTimeshare(String sql) {
		// TODO Auto-generated method stub
		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}

		try {
			PreparedStatement prestmt = conn.prepareStatement(sql);
			ResultSet resultSet = prestmt.executeQuery();
			return resultSet;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public BigInteger getPastNDaysCumVolume(String ticker, Timestamp datetime,
			int daysCount) {
		// TODO Auto-generated method stub
		// String sql =
		// "select sum(a.volume) as cumVolume from (select max(volume) as volume "
		// + "from xcube.market_quotation where contractid='"
		// + ticker
		// + "' and date_sub(date('"
		// + datetime
		// + "'), interval 6 day) < date(tradingtime) and date('"
		// + datetime
		// +
		// "') > date(tradingtime) group by contractid,date(tradingtime)) as a;";

		String sql = "select sum(volume) as cumVolume from xcube.settlement_data "
				+ "where ticker='"
				+ ticker
				+ "' and date_sub(date('"
				+ datetime
				+ "'), interval 6 day) < tradingdate and date('"
				+ datetime + "') > tradingdate;";

		if (conn == null) {
			System.out
					.println("RawDataAccess.getPastNDaysCumVolume >>> reconstruct connection");
			conn = MysqlDBUtil.getConnection();
		}

		try {
			PreparedStatement prestmt = conn.prepareStatement(sql);
			ResultSet resultSet = prestmt.executeQuery();
			BigInteger cumVolume = BigInteger.ZERO;
			while (resultSet.next()) {
				cumVolume = BigInteger.valueOf(resultSet.getLong("cumVolume"));
			}
			return cumVolume;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
