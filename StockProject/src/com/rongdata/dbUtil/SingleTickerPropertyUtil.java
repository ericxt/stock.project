package com.rongdata.dbUtil;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

enum SingleTickerProperty {
	TickerName, TickerCode, ListingDate, UpdateDate, Validity;
}

public class SingleTickerPropertyUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;

	public SingleTickerPropertyUtil() {
		// TODO Auto-generated constructor stub
	}

	public SingleTickerPropertyUtil(Connection conn) {
		this.conn = conn;
	}

	public SingleTickerPropertyUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public SingleTickerPropertyUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	public void insertAll() {
		String targetSql = "insert ignore into xcube.com_single_ticker_property(TickerName, "
				+ "TickerCode, ListingDate, UpdateDate, Validity) value(?, ?, ?, ?, ?)";
		String sourceSql = "select a.TradingTime, a.ContractId from "
				+ "(select tradingtime, contractid from xcube.market_quotation "
				+ "where contractid rlike '^S.*' order by tradingtime<=now(),"
				+ "tradingtime desc) as a group by contractid;";

		String tickerName = null; // 股票名称
		String tickerCode = null; // 股票代码
		Date listingDate = null; // 上市日期
		Date updateDate = null; // 更新日期
		int validity = 1; // 1有效 0无效

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}

		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);

			while (resultSet.next()) {
				tickerCode = resultSet.getString("ContractId");
				tickerName = tickerCode;
				listingDate = calListingDate(tickerCode);
				updateDate = calUpdateDate();
				Timestamp tradingTime = resultSet.getTimestamp("TradingTime");
				validity = calValidity(tickerCode, tradingTime);

				prestmt.setString(
						SingleTickerProperty.TickerName.ordinal() + 1,
						tickerName);
				prestmt.setString(
						SingleTickerProperty.TickerCode.ordinal() + 1,
						tickerCode);
				prestmt.setDate(SingleTickerProperty.ListingDate.ordinal() + 1,
						listingDate);
				prestmt.setDate(SingleTickerProperty.UpdateDate.ordinal() + 1,
						updateDate);
				prestmt.setInt(SingleTickerProperty.Validity.ordinal() + 1,
						validity);

				prestmt.execute();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// to be completed
	Date calListingDate(String tickerCode) {
		// TODO Auto-generated method stub
		return null;
	}

	// to be completed
	Date calUpdateDate() {
		// TODO Auto-generated method stub
		return null;
	}

	// to be completed
	int calValidity(String tickerCode, Timestamp tradingTime) {
		// TODO Auto-generated method stub
		return 0;
	}

}
