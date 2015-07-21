package com.rongdata.dbUtil;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

enum FuturesProperty {
	Ticker, TickerName, ListingDate, UpdateDate, Validity, Type, SuperFutures;
}

public class FuturesPropertyUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;

	public FuturesPropertyUtil() {
		// TODO Auto-generated constructor stub
	}

	public FuturesPropertyUtil(Connection conn) {
		this.conn = conn;
	}

	public FuturesPropertyUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public FuturesPropertyUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	public void insertAll() {
		String targetSql = "insert ignore into xcube.com_futures_property(Ticker, TickerName, "
				+ "ListingDate, UpdateDate, Validity, Type, SuperFutures) values(?, ?, ?, "
				+ "?, ?, ?, ?)";
		/**
		 * String sourceSql =
		 * "select contractid, tradingtime, preholdings from (select * from " +
		 * "xcube.market_quotation where contractid rlike '(TA|TC).*' order by "
		 * + "tradingtime<=now(),tradingtime desc) as a group by contractid;";
		 **/
		String sourceSql = "select * from (select contractid, tradingtime, preholdings "
				+ "from xcube.futures_quotation as a "
				+ "where TradingTime=(select TradingTime from xcube.latest_futures_tradingtime "
				+ "where a.ContractId=contractid)) as b group by contractid";

		String ticker = null;
		String tickerName = ticker;
		Date listingDate = null;
		Date updateDate = null;
		int validity = 1;
		int type = 0;
		String superFutures = null;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}
		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);

			while (resultSet.next()) {
				ticker = resultSet.getString("ContractId");
				tickerName = ticker;
				listingDate = calListingDate(ticker);
				updateDate = calUpdateDate();
				Timestamp tradingTime = resultSet.getTimestamp("TradingTime");
				validity = calValidity(ticker, tradingTime);
				int preHoldings = resultSet.getInt("PreHoldings");
				type = calType(preHoldings);
				superFutures = calSuperFutures();

				prestmt.setString(FuturesProperty.Ticker.ordinal() + 1, ticker);
				prestmt.setString(FuturesProperty.TickerName.ordinal() + 1,
						tickerName);
				prestmt.setDate(FuturesProperty.ListingDate.ordinal() + 1,
						listingDate);
				prestmt.setDate(FuturesProperty.UpdateDate.ordinal() + 1,
						updateDate);
				prestmt.setInt(FuturesProperty.Validity.ordinal() + 1, validity);
				prestmt.setInt(FuturesProperty.Type.ordinal() + 1, type);
				prestmt.setString(FuturesProperty.SuperFutures.ordinal() + 1,
						superFutures);

				prestmt.execute();
			}

			prestmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void update(PreparedStatement prestmt) {
		// TODO Auto-generated method stub
		String ticker = null;
		String tickerName = ticker;
		Date listingDate = null;
		Date updateDate = null;
		int validity = 1;
		int type = 0;
		String superFutures = null;

		try {
//			while (resultSet.next()) {
				ticker = resultSet.getString("ContractId");
				tickerName = ticker;
				listingDate = calListingDate(ticker);
				updateDate = calUpdateDate();
				Timestamp tradingTime = resultSet.getTimestamp("TradingTime");
				validity = calValidity(ticker, tradingTime);
				int preHoldings = resultSet.getInt("PreHoldings");
				type = calType(preHoldings);
				superFutures = calSuperFutures();

				System.out.println(resultSet.getRow()
						+ " FuturesPropertyUtil.update >>> " + ticker + ","
						+ tickerName + "," + listingDate + "," + updateDate
						+ "," + validity + "," + type + "," + superFutures);

				prestmt.setString(FuturesProperty.Ticker.ordinal() + 1, ticker);
				prestmt.setString(FuturesProperty.TickerName.ordinal() + 1,
						tickerName);
				prestmt.setDate(FuturesProperty.ListingDate.ordinal() + 1,
						listingDate);
				prestmt.setDate(FuturesProperty.UpdateDate.ordinal() + 1,
						updateDate);
				prestmt.setInt(FuturesProperty.Validity.ordinal() + 1, validity);
				prestmt.setInt(FuturesProperty.Type.ordinal() + 1, type);
				prestmt.setString(FuturesProperty.SuperFutures.ordinal() + 1,
						superFutures);

				prestmt.execute();
//			}

			// prestmt.close();
			// conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * following private methods are utility functions.
	 */

	// to be completed
	String calSuperFutures() {
		// TODO Auto-generated method stub
		return null;
	}

	// to be completed, there is no rule
	int calType(int preHoldings) {
		// TODO Auto-generated method stub
		return 0;
	}

	// to be completed, there is no rule
	int calValidity(String ticker, Timestamp tradingTime) {
		// TODO Auto-generated method stub
		return 1;
	}

	// to be completed
	Date calUpdateDate() {
		// TODO Auto-generated method stub
		return null;
	}

	// to be completed, there is no rule
	Date calListingDate(String ticker) {
		// TODO Auto-generated method stub
		return null;
	}

}
