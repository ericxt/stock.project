package com.rongdata.dbUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

enum TickerTimeshare {
	Ticker, Datetime, Price, HightAndLowRange, Volume, PositiveEmotions, TradingSentiment, WindVane;
}

public class TickerTimeshareUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;

	private PreparedStatement prestmt = null;

	public TickerTimeshareUtil() {
		// TODO Auto-generated constructor stub
	}

	public TickerTimeshareUtil(Connection conn) {
		this.conn = conn;
	}

	public TickerTimeshareUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public TickerTimeshareUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	public void insertAll() {
		// lack of public sentiment data
		// String sourceSql =
		// "select a.StockCode as Ticker, a.Datetime, a.CurrentPrice, "
		// +
		// "a.HightAndLowRange, a.Volume, b.PositiveEmotions, a.TradingSentiment, "
		// + "a.WindVane from xcube.com_single_ticker_details as a, "
		// + "xcube.com_public_sentiment as b where a.StockCode = b.Ticker "
		// + "and date(a.Datetime) = date(b.Datetime);";

		String sourceSql = "select a.StockCode as Ticker, a.Datetime, a.CurrentPrice, "
				+ "a.HightAndLowRange, a.Volume, b.PositiveEmotions, a.TradingSentiment, a.WindVane "
				+ "from xcube.com_single_ticker_details as a left join "
				+ "xcube.com_public_sentiment as b "
				+ "on (a.StockCode = b.Ticker and date(a.Datetime) = date(b.Datetime));";

		String targetSql = "replace into xcube.com_ticker_timeshare(Ticker, Datetime, "
				+ "Price, HightAndLowRange, Volume, PositiveEmotions, TradingSentiment, "
				+ "WindVane) values(?, ?, ?, ?, ?, ?, ?, ?) ";

		String ticker = null;
		Timestamp datetime = null;
		BigDecimal price = BigDecimal.ZERO; // 当前价格
		float hightAndLowRange = 0;
		BigInteger volume = BigInteger.ZERO;
		float positiveEmotions = 0;
		float tradingSentiment = 0;
		float windVane = 0;
		
		int count = 0;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		
		System.out.println(">>> TickerTimeShare Getting Result Set");
		resultSet = new RawDataAccess(conn).getTickerTimeshare(sourceSql);

		try {
			prestmt = conn.prepareStatement(targetSql);

			conn.setAutoCommit(false);
			while (resultSet.next()) {
				count++;
				
				ticker = resultSet.getString("Ticker");
				datetime = resultSet.getTimestamp("Datetime");
				price = resultSet.getBigDecimal("CurrentPrice");
				hightAndLowRange = resultSet.getFloat("HightAndLowRange");
				volume = BigInteger.valueOf(resultSet.getLong("Volume"));
				positiveEmotions = resultSet.getFloat("PositiveEmotions");
				tradingSentiment = resultSet.getFloat("TradingSentiment");
				windVane = resultSet.getFloat("WindVane");

				System.out.println("    >>> " + resultSet.getRow() + " TickerTimeShareRecord");
				prestmt.setString(TickerTimeshare.Ticker.ordinal() + 1, ticker);
				prestmt.setTimestamp(TickerTimeshare.Datetime.ordinal() + 1,
						datetime);
				prestmt.setBigDecimal(TickerTimeshare.Price.ordinal() + 1,
						price);
				prestmt.setFloat(
						TickerTimeshare.HightAndLowRange.ordinal() + 1,
						hightAndLowRange);
				prestmt.setLong(TickerTimeshare.Volume.ordinal() + 1,
						volume.longValue());
				prestmt.setFloat(
						TickerTimeshare.PositiveEmotions.ordinal() + 1,
						positiveEmotions);
				prestmt.setFloat(
						TickerTimeshare.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				prestmt.setFloat(TickerTimeshare.WindVane.ordinal() + 1,
						windVane);

//				prestmt.execute();
				prestmt.addBatch();
				
				if (count % 300 == 0) {
					prestmt.executeBatch();
					conn.commit();
					System.out.println("  >>> Update 100 TickerTimeShare Records");
				}
			}
			
			if (count % 300 != 0) {
				prestmt.executeBatch();
				conn.commit();
				System.out.println("  >>> Update " + (count % 300) + " TickerTimeShare Records");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void closeStatement() {
		try {
			prestmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
