package com.rongdata.dbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

enum FuturesPosition {
	Ticker, Datetime, Price, NowHand, LoadingUp, Nature;
}

public class FuturesPositionUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;

	public FuturesPositionUtil() {
		// TODO Auto-generated constructor stub
	}

	public FuturesPositionUtil(Connection conn) {
		this.conn = conn;
	}

	public FuturesPositionUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public FuturesPositionUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	/**
	 * extract data from com_futures_details to keep concurrent with com_futures_details
	 */
	public void insertAll() {
		String targetSql = "replace into xcube.com_futures_position(Ticker, Datetime, "
				+ "Price, NowHand, LoadingUp, Nature) values(?, ?, ?, ?, ?, ?)";
		String sourceSql = "select ticker, datetime, currentprice, nowhand, loadingup "
				+ "from xcube.com_futures_details "
				+ "union select stockcode as ticker, datetime, currentprice, nowhand, loadingup "
				+ "from xcube.com_index_details "
				+ "union select ticker, datetime, currentprice, nowhand, loadingup "
				+ "from xcube.com_debt_details;";

		String ticker = null;
		Timestamp datetime = null;
		float price = 0;
		float nowHand = 0;
		float loadingUp = 0;
		float nature = 0;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}
		
		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);
			
			while (resultSet.next()) {
				ticker = resultSet.getString("ticker");
				datetime = resultSet.getTimestamp("datetime");
				price = resultSet.getFloat("currentprice");
				nowHand = resultSet.getFloat("nowhand");
				loadingUp = resultSet.getFloat("loadingup");
				nature = calNature();
				
				prestmt.setString(FuturesPosition.Ticker.ordinal() + 1, ticker);
				prestmt.setTimestamp(FuturesPosition.Datetime.ordinal() + 1, datetime);
				prestmt.setFloat(FuturesPosition.Price.ordinal() + 1, price);
				prestmt.setFloat(FuturesPosition.NowHand.ordinal() + 1, nowHand);
				prestmt.setFloat(FuturesPosition.LoadingUp.ordinal() + 1, loadingUp);
				prestmt.setFloat(FuturesPosition.Nature.ordinal() + 1, nature);
				
				prestmt.execute();
			}
			
			prestmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// to be completed, there is no rule
	private float calNature() {
		// TODO Auto-generated method stub
		return 0;
	}

}
