package com.rongdata.dbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

enum PublicSentiment {
	Ticker, Datetime, PositiveEmotions, NegativeEmotions, PostNumber;
}

public class PublicSentimentUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;
	
	public PublicSentimentUtil() {
		// TODO Auto-generated constructor stub
	}
	
	public PublicSentimentUtil(Connection conn) {
		this.conn = conn;
	}
	
	public PublicSentimentUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}
	
	public PublicSentimentUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}
	
	public void insertAll() {
		String sourceSql = "select stock_id as ticker, from_unixtime(start_ts) as datetime, "
				+ "positive, negative, total from xcube.stock_comment";
		String targetSql = "insert ignore into xcube.com_public_sentiment(Ticker, Datetime, "
				+ "PositiveEmotions, NegativeEmotions, PostNumber) value(?, ?, ?, ?, ?);";
		
		String ticker = null;
		Timestamp datetime = null;
		float positiveEmotions = 0;
		float negativeEmotions = 0;
		int postNumber = 0;
		
		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getStockComment(sourceSql);
		}
		
		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);
			
			while (resultSet.next()) {
				ticker = resultSet.getString("ticker");
				datetime = resultSet.getTimestamp("datetime");
				positiveEmotions = calPositiveEmotions();
				negativeEmotions = calnegativeEmotions();
				postNumber = resultSet.getInt("total");
				
				prestmt.setString(PublicSentiment.Ticker.ordinal() + 1, ticker);
				prestmt.setTimestamp(PublicSentiment.Datetime.ordinal() + 1, datetime);
				prestmt.setFloat(PublicSentiment.PositiveEmotions.ordinal() + 1, positiveEmotions);
				prestmt.setFloat(PublicSentiment.NegativeEmotions.ordinal() + 1, negativeEmotions);
				prestmt.setInt(PublicSentiment.PostNumber.ordinal() + 1, postNumber);
				
				prestmt.execute();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	// to be completed
	private float calPositiveEmotions() {
		// TODO Auto-generated method stub
		return 0;
	}

	// to be completed
	private float calnegativeEmotions() {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
