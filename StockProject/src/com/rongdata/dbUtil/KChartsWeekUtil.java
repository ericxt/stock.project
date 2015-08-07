package com.rongdata.dbUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import com.mysql.jdbc.Connection;

enum KChartsWeek {
	Ticker, Datetime, OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, TradingSentiment, WindVane, MassOfPublicOpinion;
}

public class KChartsWeekUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;
	private BigDecimal openPrice = BigDecimal.ZERO;
	
	private HashMap<String, BigDecimal> tickerMap = new HashMap<String, BigDecimal>();

	private PreparedStatement prestmt = null;

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
		Date datetime = null; // 时间
		openPrice = BigDecimal.ZERO;
		BigDecimal hightPrice = BigDecimal.ZERO; // 最高价格
		BigDecimal lowPrice = BigDecimal.ZERO; // 最低价格
		BigDecimal closePrice = BigDecimal.ZERO; // 收盘价格
		BigInteger volume = BigInteger.ZERO; // 累计成交量
		BigDecimal amount = BigDecimal.ZERO; // 累计成交金额
		float tradingSentiment = 0; // 交易情绪
		float windVane = 0; // 风向标
		float massOfPublicOpinion = 0; // 大众舆情

		int count = 0;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}

		System.out.println("  >>> KChartsWeek Getting Result Set");
		resultSet = new RawDataAccess(conn).getRawData(sourceSql);

		try {
			prestmt = conn.prepareStatement(targetSql);

			conn.setAutoCommit(false);
			while (resultSet.next()) {
				count++;

				ticker = resultSet.getString("Ticker");
				datetime = resultSet.getDate("Datetime");
				if (!tickerMap.containsKey(ticker)) {
					openPrice = getWeekOpenPrice(ticker, datetime);
					tickerMap.put(ticker, openPrice);
				} else {
					openPrice = tickerMap.get(ticker);
				}
				hightPrice = resultSet.getBigDecimal("HightPrice");
				lowPrice = resultSet.getBigDecimal("LowPrice");
				closePrice = getWeekClosePrice(ticker, datetime);
				volume = BigInteger.valueOf(resultSet.getLong("Volume"));
				amount = resultSet.getBigDecimal("Amount");
				tradingSentiment = resultSet.getFloat("TradingSentiment");
				windVane = resultSet.getFloat("WindVane");
				massOfPublicOpinion = resultSet.getFloat("MassOfPublicOpinion");

				System.out.println("    >>> " + resultSet.getRow()
						+ " KChartsWeek Record");
				prestmt.setString(KChartsWeek.Ticker.ordinal() + 1, ticker);
				prestmt.setDate(KChartsWeek.Datetime.ordinal() + 1,
						datetime);
				prestmt.setBigDecimal(KChartsWeek.OpenPrice.ordinal() + 1,
						openPrice);
				prestmt.setBigDecimal(KChartsWeek.HightPrice.ordinal() + 1,
						hightPrice);
				prestmt.setBigDecimal(KChartsWeek.LowPrice.ordinal() + 1,
						lowPrice);
				prestmt.setBigDecimal(KChartsWeek.ClosePrice.ordinal() + 1,
						closePrice);
				prestmt.setLong(KChartsWeek.Volume.ordinal() + 1,
						volume.longValue());
				prestmt.setBigDecimal(KChartsWeek.Amount.ordinal() + 1, amount);
				prestmt.setFloat(KChartsWeek.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				prestmt.setFloat(KChartsWeek.WindVane.ordinal() + 1, windVane);
				prestmt.setFloat(KChartsWeek.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				// prestmt.execute();
				prestmt.addBatch();

				if (count % 200 == 0) {
					prestmt.executeBatch();
					conn.commit();
					System.out
							.println("     >>> Update 200 KChartsWeek Records");
				}
			}

			if (count % 200 != 0) {
				prestmt.executeBatch();
				conn.commit();
				System.out.println("     >>> Update " + (count % 200) + " KChartsWeek Records");
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private BigDecimal getWeekOpenPrice(String ticker, Date datetime) {
		// TODO Auto-generated method stub
		String sql = "select openprice from xcube.com_k_charts_day where ticker = '"
				+ ticker
				+ "' and datetime = subdate('"
				+ datetime
				+ "',interval weekday('" + datetime + "') day)";
		BigDecimal openPrice = BigDecimal.ZERO;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}

		try {
			System.out.println("    >>> KChartsWeekUtil.getWeekOpenPrice >>> " + sql);
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

	private BigDecimal getWeekClosePrice(String ticker, Date datetime) {
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
			System.out.println("    >>> KChartsWeekUtil.getWeekClosePrice >> " + sql);
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

	public void closeStatement() {
		try {
			prestmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
