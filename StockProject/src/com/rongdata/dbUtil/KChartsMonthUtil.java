package com.rongdata.dbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import com.mysql.jdbc.Connection;

enum KChartsMonth {
	Ticker, Datetime, OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, TradingSentiment, WindVane, MassOfPublicOpinion;
}

public class KChartsMonthUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;
	private boolean firstMonthOpenPriceOperation = true;

	public KChartsMonthUtil() {
		// TODO Auto-generated constructor stub
	}

	public KChartsMonthUtil(Connection conn) {
		this.conn = conn;
	}

	public KChartsMonthUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public KChartsMonthUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	/**
	 * store the data of each month, from first day to the last day
	 */
	public void insertAll() {
		String targetSql = "replace into xcube.com_k_charts_month(Ticker, Datetime, "
				+ "OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		// String sourceSql =
		// "select ticker,datetime,max(hightprice) as hightprice,"
		// + "min(lowprice) as lowprice,sum(volume) as volume,"
		// +
		// "sum(amount) as amount,tradingsentiment,windvane,massofpublicopinion "
		// + "from xcube.com_k_charts_day group by ticker, month(datetime);";

		String sourceSql = "select ticker,max(datetime) as datetime,max(hightprice) as hightprice,"
				+ "min(lowprice) as lowprice,sum(volume) as volume,"
				+ "sum(amount) as amount,tradingsentiment,windvane,massofpublicopinion "
				+ "from xcube.com_k_charts_day group by ticker having month(datetime)=month(now());";

		String ticker = null; // 证券代码
		Timestamp datetime = null; // 时间
		float openPrice = 0; // 开盘价
		float hightPrice = 0; // 最高价格
		float lowPrice = 0; // 最低价格
		float closePrice = 0; // 收盘价格
		float volume = 0; // 累计成交量
		float amount = 0; // 累计成交金额
		float tradingSentiment = 0; // 交易情绪
		float windVane = 0; // 风向标
		float massOfPublicOpinion = 0; // 大众舆情

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}

		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);

			while (resultSet.next()) {
				ticker = resultSet.getString("Ticker");
				datetime = resultSet.getTimestamp("Datetime");
				if (firstMonthOpenPriceOperation) {
					openPrice = getMonthOpenPrice(ticker, datetime);
					firstMonthOpenPriceOperation = false;
				}
				hightPrice = resultSet.getFloat("HightPrice");
				lowPrice = resultSet.getFloat("LowPrice");
				closePrice = getMonthClosePrice(ticker, datetime);
				volume = resultSet.getFloat("Volume");
				amount = resultSet.getFloat("Amount");
				tradingSentiment = resultSet.getFloat("TradingSentiment");
				windVane = resultSet.getFloat("WindVane");
				massOfPublicOpinion = resultSet.getFloat("MassOfPublicOpinion");

				prestmt.setString(KChartsMonth.Ticker.ordinal() + 1, ticker);
				prestmt.setTimestamp(KChartsMonth.Datetime.ordinal() + 1,
						datetime);
				prestmt.setFloat(KChartsMonth.OpenPrice.ordinal() + 1,
						openPrice);
				prestmt.setFloat(KChartsMonth.HightPrice.ordinal() + 1,
						hightPrice);
				prestmt.setFloat(KChartsMonth.LowPrice.ordinal() + 1, lowPrice);
				prestmt.setFloat(KChartsMonth.ClosePrice.ordinal() + 1,
						closePrice);
				prestmt.setFloat(KChartsMonth.Volume.ordinal() + 1, volume);
				prestmt.setFloat(KChartsMonth.Amount.ordinal() + 1, amount);
				prestmt.setFloat(KChartsMonth.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				prestmt.setFloat(KChartsMonth.WindVane.ordinal() + 1, windVane);
				prestmt.setFloat(
						KChartsMonth.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				prestmt.execute();
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private float getMonthOpenPrice(String ticker, Timestamp datetime) {
		// calculate the first weekday of the month
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(datetime.getTime());
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		int dayofweek = calendar.get(Calendar.DAY_OF_WEEK);
		if (dayofweek == 0) {
			calendar.set(Calendar.DAY_OF_MONTH, 2);
		} else if (dayofweek == 7) {
			calendar.set(Calendar.DAY_OF_MONTH, 3);
		}
		int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
		
		String sql = "select openprice from xcube.com_k_charts_day where ticker = '"
				+ ticker
				+ "' and datetime = subdate(date('"
				+ datetime
				+ "'),interval " + (dayOfMonth - 1) + " day)";
		float openPrice = 0;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}

		try {
			PreparedStatement prestmt = conn.prepareStatement(sql);
			ResultSet rest = prestmt.executeQuery();
			while (rest.next()) {
				openPrice = rest.getFloat("openprice");
			}
			return openPrice;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return Float.MIN_NORMAL;
	}

	private float getMonthClosePrice(String ticker, Timestamp datetime) {
		String sql = "select closeprice from xcube.com_k_charts_day where ticker = '"
				+ ticker + "' and datetime='" + datetime + "';";
		float closePrice = 0;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}

		try {
			PreparedStatement prestmt = conn.prepareStatement(sql);
			ResultSet rest = prestmt.executeQuery();
			while (rest.next()) {
				closePrice = rest.getFloat("closeprice");
			}
			return closePrice;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return Float.MIN_NORMAL;
	}

}
