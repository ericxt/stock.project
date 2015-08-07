package com.rongdata.dbUtil;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashSet;

import com.mysql.jdbc.Connection;

enum IndexProperty {
	Ticker, TickerName, ListingDate, UpdateDate, Validity, Type, PTicker;
}

public class IndextPropertyUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;

	public IndextPropertyUtil() {
		// TODO Auto-generated constructor stub
	}

	public IndextPropertyUtil(Connection conn) {
		this.conn = conn;
	}

	public IndextPropertyUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public IndextPropertyUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	public void insertAll() {
		String targetSql = "replace into xcube.com_index_property(Ticker, TickerName, "
				+ "ListingDate, UpdateDate, Validity, Type, PTicker) values(?, ?, ?, "
				+ "?, ?, ?, ?)";
		String sourceSql = "select contractid, preholdings, tradingtime from "
				+ "(select * from xcube.market_quotation "
				+ "where contractid rlike '^I.*' order by tradingtime<=now(),"
				+ "tradingtime desc) as a group by contractid;";

		String ticker = null; // 期指代码
		String tickerName = null; // 期指名称
		Date listingDate = null; // 上市日期
		Date updateDate = null; // 更新日期
		int validity = 1; // 1有效 0无效
		int type = 1; // 1主力合约 2 非主力合约
		String pTicker = null; // 父期指

		HashSet<String> mainTickerSet = new HashSet<String>();
		// int mainTickerHoldings = 0;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}

		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);
			mainTickerSet = getMainTicker();

			while (resultSet.next()) {
				ticker = resultSet.getString("ContractId");
				tickerName = ticker;
				listingDate = calListingDate(ticker);
				updateDate = calUpdateDate();
				Timestamp tradingTime = resultSet.getTimestamp("TradingTime");
				validity = calValidity(ticker, tradingTime);
				// int preHoldings = resultSet.getInt("PreHoldings");
				type = (mainTickerSet.contains(ticker)) ? 1 : 0;
				pTicker = calPTicker();

				prestmt.setString(IndexProperty.Ticker.ordinal() + 1, ticker);
				prestmt.setString(IndexProperty.TickerName.ordinal() + 1,
						tickerName);
				prestmt.setDate(IndexProperty.ListingDate.ordinal() + 1,
						listingDate);
				prestmt.setDate(IndexProperty.UpdateDate.ordinal() + 1,
						updateDate);
				prestmt.setInt(IndexProperty.Validity.ordinal() + 1, validity);
				prestmt.setInt(IndexProperty.Type.ordinal() + 1, type);
				prestmt.setString(IndexProperty.PTicker.ordinal() + 1, pTicker);

				prestmt.execute();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	HashSet<String> getMainTicker() {
		// TODO Auto-generated method stub
		// String sql = "select contractid, preholdings, tradingtime from "
		// + "(select * from xcube.market_quotation "
		// + "where contractid rlike '^I.*' order by tradingtime<=now(),"
		// + "tradingtime desc) as a group by contractid;";

		String sql = "select * from (select ContractId, PreHoldings, tradingtime "
				+ "from xcube.index_quotation as a "
				+ "where TradingTime=(select TradingTime from xcube.latest_index_tradingtime "
				+ "where a.ContractId=contractid)) as b group by contractid";
		HashSet<String> mainTickerSet = new HashSet<String>();
		String mainICTicker = null;
		String mainIFTicker = null;
		int ICTickerHoldings = 0;
		int IFTickerHoldings = 0;
		try {
			PreparedStatement prestmt = conn.prepareStatement(sql);
			ResultSet rest = prestmt.executeQuery();
			while (rest.next()) {
				String ticker = rest.getString("contractid");
				int preHoldings = rest.getInt("PreHoldings");
				if (ticker.startsWith("IC")) {
					if (preHoldings >= ICTickerHoldings) {
						ICTickerHoldings = preHoldings;
						mainICTicker = ticker;
					}
				} else if (ticker.startsWith("IF")) {
					if (preHoldings >= IFTickerHoldings) {
						IFTickerHoldings = preHoldings;
						mainIFTicker = ticker;
					}
				}
			}
			mainTickerSet.add(mainIFTicker);
			mainTickerSet.add(mainICTicker);

			return mainTickerSet;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	Date calListingDate(String ticker) {
		// TODO Auto-generated method stub
		String tickerDate = ticker.substring(2);
		int deliveryTime = Integer.parseInt(tickerDate);
		int deliveryYear = deliveryTime / 100 + 2000;
		int deliveryMonth = deliveryTime % 100;
		if (deliveryMonth % 3 != 0) {
			Calendar calendar = Calendar.getInstance();
			calendar.set(Calendar.YEAR, deliveryYear);
			calendar.set(Calendar.MONTH, deliveryMonth - 1 - 2);
			calendar.set(Calendar.WEEK_OF_MONTH, 4);
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
			long timeInMillis = calendar.getTimeInMillis();
			Date listingDate = new Date(timeInMillis);
			return listingDate;
		} else {
			Calendar calendar = Calendar.getInstance();
			calendar.set(Calendar.YEAR, deliveryYear);
			calendar.set(Calendar.MONTH, deliveryMonth - 1 - 8);
			calendar.set(Calendar.WEEK_OF_MONTH, 4);
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
			long timeInMillis = calendar.getTimeInMillis();
			Date listingDate = new Date(timeInMillis);
			return listingDate;
		}
	}

	// to be completed
	Date calUpdateDate() {
		// TODO Auto-generated method stub
		return null;
	}

	int calValidity(String ticker, Timestamp tradingTime) {
		// TODO Auto-generated method stub
		int deliveryTime = Integer.parseInt(ticker.substring(2));
		int deliveryYear = deliveryTime / 100 + 2000;
		int deliveryMonth = deliveryTime % 100;
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, deliveryYear);
		calendar.set(Calendar.MONTH, deliveryMonth - 1);
		calendar.set(Calendar.WEEK_OF_MONTH, 3);
		calendar.set(Calendar.DAY_OF_WEEK, Calendar.FRIDAY);
		int deliveryDay = calendar.get(Calendar.DAY_OF_MONTH);

		long tradingTimeMillis = tradingTime.getTime();
		calendar.setTimeInMillis(tradingTimeMillis);
		int tradingYear = calendar.get(Calendar.YEAR);
		int tradingMonth = calendar.get(Calendar.MONTH) + 1;
		int tradingDay = calendar.get(Calendar.DAY_OF_MONTH);
		// System.out.println(deliveryDate + ", " + deliveryTimeMillis + ", " +
		// tradingTimeMillis);
		if (deliveryYear > tradingYear || deliveryMonth > tradingMonth
				|| (deliveryMonth == tradingMonth && deliveryDay > tradingDay)) {
			return 1;
		}
		return 0;
	}

	// to be completed
	String calPTicker() {
		// TODO Auto-generated method stub
		return null;
	}

}
