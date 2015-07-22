package com.rongdata.dbUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

enum KChartsWeek {
	Ticker, Datetime, OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, TradingSentiment, WindVane, MassOfPublicOpinion;
}

public class KChartsWeekUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;
	private boolean firstWeekOpenPriceOperation = true;
	private BigDecimal openPrice;

	public KChartsWeekUtil() {
		// TODO Auto-generated constructor stub
	}

	public KChartsWeekUtil(Connection conn) {
		this.conn = conn;
	}

	public KChartsWeekUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public KChartsWeekUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	/**
	 * store the data of each week, from Monday to Friday
	 */
	public void insertAll() {
		String targetSql = "replace into xcube.com_k_charts_week(Ticker, Datetime, "
				+ "OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		// String sourceSql =
		// "select ticker,datetime,max(hightprice) as hightprice,"
		// + "min(lowprice) as lowprice,sum(volume) as volume,"
		// +
		// "sum(amount) as amount,tradingsentiment,windvane,massofpublicopinion "
		// + "from xcube.com_k_charts_day group by ticker, week(datetime);";

		String sourceSql = "select ticker,max(datetime) as datetime,max(hightprice) as hightprice,"
				+ "min(lowprice) as lowprice,sum(volume) as volume,"
				+ "sum(amount) as amount,tradingsentiment,windvane,massofpublicopinion "
				+ "from xcube.com_k_charts_day group by ticker having week(datetime)=week(now());";

		String ticker = null; // 证券代码
		Timestamp datetime = null; // 时间
		openPrice = BigDecimal.ZERO;
		BigDecimal hightPrice = BigDecimal.ZERO; // 最高价格
		BigDecimal lowPrice = BigDecimal.ZERO; // 最低价格
		BigDecimal closePrice = BigDecimal.ZERO; // 收盘价格
		BigInteger volume = BigInteger.ZERO; // 累计成交量
		BigDecimal amount = BigDecimal.ZERO; // 累计成交金额
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
				if (firstWeekOpenPriceOperation) {
					openPrice = getWeekOpenPrice(ticker, datetime);
					firstWeekOpenPriceOperation = false;
				}
				hightPrice = resultSet.getBigDecimal("HightPrice");
				lowPrice = resultSet.getBigDecimal("LowPrice");
				closePrice = getWeekClosePrice(ticker, datetime);
				volume = BigInteger.valueOf(resultSet.getLong("Volume"));
				amount = resultSet.getBigDecimal("Amount");
				tradingSentiment = resultSet.getFloat("TradingSentiment");
				windVane = resultSet.getFloat("WindVane");
				massOfPublicOpinion = resultSet.getFloat("MassOfPublicOpinion");

				prestmt.setString(KChartsWeek.Ticker.ordinal() + 1, ticker);
				prestmt.setTimestamp(KChartsWeek.Datetime.ordinal() + 1,
						datetime);
				prestmt.setBigDecimal(KChartsWeek.OpenPrice.ordinal() + 1, openPrice);
				prestmt.setBigDecimal(KChartsWeek.HightPrice.ordinal() + 1,
						hightPrice);
				prestmt.setBigDecimal(KChartsWeek.LowPrice.ordinal() + 1, lowPrice);
				prestmt.setBigDecimal(KChartsWeek.ClosePrice.ordinal() + 1,
						closePrice);
				prestmt.setLong(KChartsWeek.Volume.ordinal() + 1, volume.longValue());
				prestmt.setBigDecimal(KChartsWeek.Amount.ordinal() + 1, amount);
				prestmt.setFloat(KChartsWeek.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				prestmt.setFloat(KChartsWeek.WindVane.ordinal() + 1, windVane);
				prestmt.setFloat(KChartsWeek.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				prestmt.execute();
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private BigDecimal getWeekOpenPrice(String ticker, Timestamp datetime) {
		// TODO Auto-generated method stub
		String sql = "select openprice from xcube.com_k_charts_day where ticker = '"
				+ ticker
				+ "' and datetime = subdate(date('"
				+ datetime
				+ "'),interval weekday('" + datetime + "') day)";
		BigDecimal openPrice = BigDecimal.ZERO;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}

		try {
			PreparedStatement prestmt = conn.prepareStatement(sql);
			ResultSet rest = prestmt.executeQuery();
			while (rest.next()) {
				openPrice = rest.getBigDecimal("openprice");
			}
			return openPrice;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return openPrice;
	}

	private BigDecimal getWeekClosePrice(String ticker, Timestamp datetime) {
		// TODO Auto-generated method stub
		// String sql =
		// "select closeprice from xcube.com_k_charts_day where ticker = '"
		// + ticker
		// + "' and week(datetime)=week('"
		// + datetime
		// + "')"
		// + " order by datetime desc;";

		String sql = "select closeprice from xcube.com_k_charts_day where ticker = '"
				+ ticker + "' and datetime='" + datetime + "'";
		BigDecimal closePrice = BigDecimal.ZERO;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}

		try {
			PreparedStatement prestmt = conn.prepareStatement(sql);
			ResultSet rest = prestmt.executeQuery();
			while (rest.next()) {
				closePrice = rest.getBigDecimal("closeprice");
			}
			return closePrice;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return closePrice;
	}

}
