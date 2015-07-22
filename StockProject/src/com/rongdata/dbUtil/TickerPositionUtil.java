package com.rongdata.dbUtil;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

enum TickerPosition {
	Ticker, Datetime, Price, NowHand;
}

public class TickerPositionUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;

	public TickerPositionUtil() {
		// TODO Auto-generated constructor stub
	}

	public TickerPositionUtil(Connection conn) {
		this.conn = conn;
	}

	public TickerPositionUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public TickerPositionUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	public void insertAll() {
		String targetSql = "replace into xcube.com_ticker_position(Ticker, Datetime, "
				+ "Price, NowHand) value(?, ?, ?, ?)";
		String sourceSql = "select TradingTime, ContractId, LatestPrice, Volume "
				+ "from (select TradingTime, ContractId, LatestPrice, Volume "
				+ "from xcube.market_quotation "
				+ "where contractid rlike '^S.*' order by tradingtime<=now(),"
				+ "tradingtime desc) as a group by contractid;";
		
		String ticker = null;
		Timestamp datetime = null;
		float price = 0;
		float nowHand = 0;
		
		int count = 0;
		
		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}
		
		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);
			conn.setAutoCommit(false);
			
			while (resultSet.next()) {
				ticker = resultSet.getString("ContractId");
				datetime = resultSet.getTimestamp("TradingTime");
				price = resultSet.getFloat("LatestPrice");
				float curVolumeCount = resultSet.getFloat("Volume");
				nowHand = calNowHand(ticker, datetime, curVolumeCount);
				
				prestmt.setString(TickerPosition.Ticker.ordinal() + 1, ticker);
				prestmt.setTimestamp(TickerPosition.Datetime.ordinal() + 1, datetime);
				prestmt.setFloat(TickerPosition.Price.ordinal() + 1, price);
				prestmt.setFloat(TickerPosition.NowHand.ordinal() + 1, nowHand);
				count++;
				prestmt.addBatch();
				if (count % 500 == 0) {
					prestmt.executeBatch();
					conn.commit();
					System.out.println("tickerpositionutil.insertall >>> " + count);
				}
//				prestmt.execute();
			}
			if (count % 500 != 0) {
				prestmt.executeBatch();
				conn.commit();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * calNowHand Float Version
	 * @param ticker
	 * @param datetime
	 * @param curVolumeCount
	 * @return
	 */
	float calNowHand(String ticker, Timestamp datetime,
			float curVolumeCount) {
		// TODO Auto-generated method stub
		float prevCumVolume = new RawDataAccess(conn).getPrevCumVolume(ticker, datetime, "stock").floatValue();
		return curVolumeCount - prevCumVolume;
	}

	/**
	 * calNowHand BigInteger Version
	 * @param stockCode
	 * @param datetime
	 * @param volume
	 * @return
	 */
	public BigInteger calNowHand(String ticker, Timestamp datetime,
			BigInteger curVolumeCount) {
		// TODO Auto-generated method stub
		BigInteger prevCumVolume = new RawDataAccess(conn).getPrevCumVolume(ticker, datetime, "stock");
		return curVolumeCount.subtract(prevCumVolume);
	}

}
