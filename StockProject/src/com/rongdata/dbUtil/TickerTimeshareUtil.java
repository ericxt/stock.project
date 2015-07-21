package com.rongdata.dbUtil;

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
		String sourceSql = "select a.StockCode as Ticker, a.Datetime, a.CurrentPrice, "
				+ "a.HightAndLowRange, a.Volume, b.PositiveEmotions, a.TradingSentiment, "
				+ "a.WindVane from xcube.com_single_ticker_details as a, "
				+ "xcube.com_public_sentiment as b where substr(a.StockCode, 3) = b.Ticker "
				+ "and date(a.Datetime) = date(b.Datetime);";
		
		String targetSql = "replace into xcube.com_ticker_timeshare(Ticker, Datetime, "
				+ "Price, HightAndLowRange, Volume, PositiveEmotions, TradingSentiment, "
				+ "WindVane) value(?, ?, ?, ?, ?, ?, ?, ?);";
		
		String ticker = null;
		Timestamp datetime = null;
		float price = 0;	// 当前价格
		float hightAndLowRange = 0;
		float volume = 0;
		float positiveEmotions = 0;
		float tradingSentiment = 0;
		float windVane = 0;
		
		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getTickerTimeshare(sourceSql);
		}
		
		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);
			
			while (resultSet.next()) {
				ticker = resultSet.getString("Ticker");
				datetime = resultSet.getTimestamp("Datetime");
				price = resultSet.getFloat("CurrentPrice");
				hightAndLowRange = resultSet.getFloat("HightAndLowRange");
				volume = resultSet.getFloat("Volume");
				positiveEmotions = resultSet.getFloat("PositiveEmotions");
				tradingSentiment = resultSet.getFloat("TradingSentiment");
				windVane = resultSet.getFloat("WindVane");
				
				prestmt.setString(TickerTimeshare.Ticker.ordinal() + 1, ticker);
				prestmt.setTimestamp(TickerTimeshare.Datetime.ordinal() + 1, datetime);
				prestmt.setFloat(TickerTimeshare.Price.ordinal() + 1, price);
				prestmt.setFloat(TickerTimeshare.HightAndLowRange.ordinal() + 1, hightAndLowRange);
				prestmt.setFloat(TickerTimeshare.Volume.ordinal() + 1, volume);
				prestmt.setFloat(TickerTimeshare.PositiveEmotions.ordinal() + 1, positiveEmotions);
				prestmt.setFloat(TickerTimeshare.TradingSentiment.ordinal() + 1, tradingSentiment);
				prestmt.setFloat(TickerTimeshare.WindVane.ordinal() + 1, windVane);
				
				prestmt.execute();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
