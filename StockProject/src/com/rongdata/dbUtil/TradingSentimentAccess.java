package com.rongdata.dbUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TradingSentimentAccess {
	private Connection conn = null;

	public TradingSentimentAccess() {
		// TODO Auto-generated constructor stub
	}

	public TradingSentimentAccess(Connection conn) {
		this.conn = conn;
	}

	public float getTradingSentiment(String ticker, Timestamp datetime) {
		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		String sql = "select tradingsentiment from xcube.com_trading_sentiment where ticker = '"
				+ ticker + "' and tradingtime = '" + datetime + "';";
		
		System.out.println("TradingSentimentAccess.gettradingsentiment >>> " + sql);
		try {
			PreparedStatement prestmt = conn.prepareStatement(sql);
			ResultSet rest = prestmt.executeQuery();
			float tradingSentiment = 0;
			while (rest.next()) {
				tradingSentiment = rest.getFloat("TAIndex");
			}
			return tradingSentiment;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return Float.MIN_NORMAL;
	}

}
