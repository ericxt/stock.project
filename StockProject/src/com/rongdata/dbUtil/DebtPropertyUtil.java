package com.rongdata.dbUtil;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashSet;

import com.mysql.jdbc.Connection;

enum DebtProperty {
	Ticker, TickerName, ListingDate, UpdateDate, Validity, Type, SuperFutures;
}

public class DebtPropertyUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;
	private HashSet<String> mainTickerSet = new HashSet<String>();

	public DebtPropertyUtil() {
		// TODO Auto-generated constructor stub
	}

	public DebtPropertyUtil(Connection conn) {
		this.conn = conn;
	}

	public DebtPropertyUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public DebtPropertyUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	public void insertAll() {
		String targetSql = "replace into xcube.com_debt_property(Ticker, TickerName, "
				+ "ListingDate, UpdateDate, Validity, Type, SuperFutures) values(?, ?, ?, "
				+ "?, ?, ?, ?);";
		String sourceSql = "select ContractId, TradingTime from "
				+ "(select * from xcube.market_quotation "
				+ "where contractid rlike '^(TF|T)[0-9].*' order by tradingtime<=now(),"
				+ "tradingtime desc) as a group by contractid;";

		String ticker = null;
		String tickerName = ticker;
		Date listingDate = null;
		Date updateDate = null;
		int validity = 1;
		int type = 0;
		String superFutures = null;

//		HashSet<String> mainTickerSet = new HashSet<String>();

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}
		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);

			mainTickerSet  = getMainTicker();

			while (resultSet.next()) {
				ticker = resultSet.getString("ContractId");
				tickerName = ticker;
				listingDate = calListingDate(ticker);
				updateDate = calUpdateDate();
				Timestamp tradingTime = resultSet.getTimestamp("TradingTime");
				validity = calValidity(ticker, tradingTime);
				// int preHoldings = resultSet.getInt("PreHoldings");
				type = (mainTickerSet.contains(ticker)) ? 1 : 0;
				superFutures = calSuperFutures();

				prestmt.setString(DebtProperty.Ticker.ordinal() + 1, ticker);
				prestmt.setString(DebtProperty.TickerName.ordinal() + 1,
						tickerName);
				prestmt.setDate(DebtProperty.ListingDate.ordinal() + 1,
						listingDate);
				prestmt.setDate(DebtProperty.UpdateDate.ordinal() + 1,
						updateDate);
				prestmt.setInt(DebtProperty.Validity.ordinal() + 1, validity);
				prestmt.setInt(DebtProperty.Type.ordinal() + 1, type);
				prestmt.setString(DebtProperty.SuperFutures.ordinal() + 1,
						superFutures);

				prestmt.execute();
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	HashSet<String> getMainTicker() {
		// TODO Auto-generated method stub
//		String sql = "select contractid, preholdings from "
//				+ "(select * from xcube.market_quotation "
//				+ "where contractid rlike '^(TF|T)[0-9].*' order by tradingtime<=now(),"
//				+ "tradingtime desc) as a group by contractid;";
		
//		String sql = "select * from (select ContractId, PreHoldings from xcube.debt_quotation as a "
//				+ "where TradingTime=(select TradingTime from xcube.latest_debt_tradingtime "
//				+ "where a.ContractId=contractid)) as b group by contractid";
		
		String sql = "select ContractId, PreHoldings from "
				+ "(select ContractId, PreHoldings from xcube.debt_quotation "
				+ "order by tradingtime desc) as a group by contractid;";
		HashSet<String> mainTickerSet = new HashSet<String>();
		String mainTTicker = null;
		String mainTFTicker = null;
		int TTickerHoldings = 0;
		int TFTickerHoldings = 0;
		try {
			PreparedStatement prestmt = conn.prepareStatement(sql);
			ResultSet rest = prestmt.executeQuery();
			while (rest.next()) {
				String ticker = rest.getString("contractid");
				int preHoldings = rest.getInt("PreHoldings");
				if (ticker.startsWith("TF")) {
					if (preHoldings >= TFTickerHoldings) {
						TFTickerHoldings = preHoldings;
						mainTFTicker = ticker;
					}
				} else {
					if (preHoldings >= TTickerHoldings) {
						TTickerHoldings = preHoldings;
						mainTTicker = ticker;
					}
				}
			}
			mainTickerSet.add(mainTFTicker);
			mainTickerSet.add(mainTTicker);
			System.out.println("debtpropertyutil.getmainticker >>> "
					+ mainTTicker + ", " + mainTFTicker);

			prestmt.close();

			return mainTickerSet;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	// to be completed
	String calSuperFutures() {
		// TODO Auto-generated method stub
		return null;
	}

	// there is no need
	private int calType(int preHoldings) {
		// TODO Auto-generated method stub
		return 0;
	}

	int calValidity(String ticker, Timestamp tradingTime) {
		// TODO Auto-generated method stub
		int deliveryTime = 0;
		if (ticker.startsWith("TF")) {
			deliveryTime = Integer.parseInt(ticker.substring(2));
		} else {
			deliveryTime = Integer.parseInt(ticker.substring(1));
		}
		int deliveryYear = deliveryTime / 100 + 2000;
		int deliveryMonth = deliveryTime % 100;
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, deliveryYear);
		calendar.set(Calendar.MONTH, deliveryMonth - 1);
		calendar.set(Calendar.WEEK_OF_MONTH, 2);
		calendar.set(Calendar.DAY_OF_WEEK, Calendar.FRIDAY);
		deliveryYear = calendar.get(Calendar.YEAR);
		int deliveryDay = calendar.get(Calendar.DAY_OF_MONTH);
		long deliveryTimeMillis = calendar.getTimeInMillis();
		Date deliveryDate = new Date(deliveryTimeMillis);

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
	Date calUpdateDate() {
		// TODO Auto-generated method stub
		return null;
	}

	Date calListingDate(String ticker) {
		// TODO Auto-generated method stub
		String tickerDate = null;
		if (ticker.startsWith("TF")) {
			tickerDate = ticker.substring(2);
		} else {
			tickerDate = ticker.substring(1);
		}
		String tickerType = ticker.substring(0, 2);
		int deliveryMonth = Integer.parseInt(tickerDate);
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, deliveryMonth / 100 + 2000);
		calendar.set(Calendar.MONTH, deliveryMonth % 100 - 1 - 9);
		calendar.set(Calendar.WEEK_OF_MONTH, 3);
		calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		long timeInMillis = calendar.getTimeInMillis();
		Date listingDate = new Date(timeInMillis);
		return listingDate;
	}

}
