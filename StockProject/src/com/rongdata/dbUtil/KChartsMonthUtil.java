package com.rongdata.dbUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;

import com.mysql.jdbc.Connection;

enum KChartsMonth {
	Ticker, Datetime, OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, TradingSentiment, WindVane, MassOfPublicOpinion;
}

public class KChartsMonthUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;
	private HashMap<String, BigDecimal> tickerMap = new HashMap<String, BigDecimal>();
	
	private PreparedStatement prestmt = null;

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
		Date datetime = null; // 时间
		BigDecimal openPrice = BigDecimal.ZERO; // 开盘价
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

		System.out.println("  >>> KChartsMonth Getting Result Set");
		resultSet = new RawDataAccess(conn).getRawData(sourceSql);

		try {
			prestmt = conn.prepareStatement(targetSql);

			conn.setAutoCommit(false);
			while (resultSet.next()) {
				count++;
				
				ticker = resultSet.getString("Ticker");
				datetime = resultSet.getDate("Datetime");
				if (!tickerMap.containsKey(ticker)) {
					openPrice = getMonthOpenPrice(ticker, datetime);
					tickerMap.put(ticker, openPrice);
				} else {
					openPrice = tickerMap.get(ticker);
				}
				hightPrice = resultSet.getBigDecimal("HightPrice");
				lowPrice = resultSet.getBigDecimal("LowPrice");
				closePrice = getMonthClosePrice(ticker, datetime);
				volume = BigInteger.valueOf(resultSet.getLong("Volume"));
				amount = resultSet.getBigDecimal("Amount");
				tradingSentiment = resultSet.getFloat("TradingSentiment");
				windVane = resultSet.getFloat("WindVane");
				massOfPublicOpinion = resultSet.getFloat("MassOfPublicOpinion");

				System.out.println("    >>> " + resultSet.getRow() + " KChartsMonth Record");
				prestmt.setString(KChartsMonth.Ticker.ordinal() + 1, ticker);
				prestmt.setDate(KChartsMonth.Datetime.ordinal() + 1,
						datetime);
				prestmt.setBigDecimal(KChartsMonth.OpenPrice.ordinal() + 1,
						openPrice);
				prestmt.setBigDecimal(KChartsMonth.HightPrice.ordinal() + 1,
						hightPrice);
				prestmt.setBigDecimal(KChartsMonth.LowPrice.ordinal() + 1,
						lowPrice);
				prestmt.setBigDecimal(KChartsMonth.ClosePrice.ordinal() + 1,
						closePrice);
				prestmt.setLong(KChartsMonth.Volume.ordinal() + 1,
						volume.longValue());
				prestmt.setBigDecimal(KChartsMonth.Amount.ordinal() + 1, amount);
				prestmt.setFloat(KChartsMonth.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				prestmt.setFloat(KChartsMonth.WindVane.ordinal() + 1, windVane);
				prestmt.setFloat(
						KChartsMonth.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

//				prestmt.execute();
				prestmt.addBatch();
				
				if (count % 200 == 0) {
					prestmt.executeBatch();
					conn.commit();
					System.out.println("Update 200 KChartsMonth Records");
				}
			}
			
			if (count % 200 != 0) {
				prestmt.executeBatch();
				conn.commit();
				System.out.println("Update " + (count % 200) + " KChartsMonth Records");
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private BigDecimal getMonthOpenPrice(String ticker, Date datetime) {
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
				+ "' and datetime = subdate('"
				+ datetime
				+ "',interval " + (dayOfMonth - 1) + " day)";
		BigDecimal openPrice = BigDecimal.ZERO;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}

		try {
			System.out.println("     >>> KChartsMonthUtil.getMonthOpenPrice >>> " + sql);
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

	private BigDecimal getMonthClosePrice(String ticker, Date datetime) {
		String sql = "select closeprice from xcube.com_k_charts_day where ticker = '"
				+ ticker + "' and datetime='" + datetime + "';";
		BigDecimal closePrice = BigDecimal.ZERO;

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}

		try {
			System.out.println("     >>> KChartsMonthUtil.getMonthClosePrice >>> " + sql);
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
