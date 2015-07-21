package com.rongdata.dbUtil;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import com.mysql.jdbc.Connection;

enum FuturesDetails {
	Ticker, CurrentPrice, HightAndLow, HightAndLowRange, Fiveupdown, YearToDate, Datetime, 
	YesterdaySettle, OpenPrice, NowHand, VolumnCount, HighPrice, LowPrice, LoadingUp, Position, 
	Volume, Turnover, EstimationSettle, OuterDisk, Disk, TradingSentiment, WindVane, MassOfPublicOpinion;

}

public class FuturesDetailsUtil {
	Connection conn = null;
	ResultSet resultSet = null;

	boolean firstFiveUpdownOperation = true;
	float fiveUpdownPrice = Float.MAX_VALUE;
	private boolean firstYearToDateOperation = true;
	private float yearToDatePrice = Float.MAX_VALUE;

	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

	public ResultSet getResultSet() {
		return resultSet;
	}

	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public FuturesDetailsUtil() {
		// TODO Auto-generated constructor stub
	}

	public FuturesDetailsUtil(Connection conn) {
		this.conn = conn;
	}

	public FuturesDetailsUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public FuturesDetailsUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	/**
	 * for TA,TC
	 */
	public void insertAll() {
		// String truncateSql = "truncate xcube.com_futures_details;";
		String targetSql = "insert ignore into xcube.com_futures_details(Ticker, CurrentPrice, "
				+ "HightAndLow, HightAndLowRange, 5updown, YearToDate, Datetime, "
				+ "YesterdaySettle, OpenPrice, NowHand, VolumnCount, HighPrice, LowPrice, "
				+ "LoadingUp, Position, Volume, EstimationSettle, OuterDisk, Disk, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) values(?, ?, ?, "
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		// the latest record group by contractid
		/**
		 * String sourceSql =
		 * "select TradingTime, ContractId, PreSettlementPrice, " +
		 * "CurrSettlementPrice, CurrOpenPrice, Holdings, LatestPrice, Volume, "
		 * +
		 * "TopPrice, BottomPrice, BidPrice1, AskPrice1, BidVolume1, AskVolume1, "
		 * +
		 * "BidPrice2, AskPrice2, BidVolume2, AskVolume2, BidPrice3, AskPrice3, "
		 * + "BidVolume3, AskVolume3 from " +
		 * "(select TradingTime, ContractId, PreSettlementPrice, " +
		 * "CurrSettlementPrice, CurrOpenPrice, Holdings, LatestPrice, Volume, "
		 * +
		 * "TopPrice, BottomPrice, BidPrice1, AskPrice1, BidVolume1, AskVolume1, "
		 * +
		 * "BidPrice2, AskPrice2, BidVolume2, AskVolume2, BidPrice3, AskPrice3, "
		 * + "BidVolume3, AskVolume3 from xcube.market_quotation " +
		 * "where contractid rlike '(TA|TC).*' order by tradingtime<=now()," +
		 * "tradingtime desc) as a group by contractid;";
		 **/
		/**
		 * String sourceSql =
		 * "select TradingTime, ContractId, PreSettlementPrice, " +
		 * "CurrSettlementPrice, CurrOpenPrice, Holdings, LatestPrice, Volume, "
		 * +
		 * "TopPrice, BottomPrice, BidPrice1, AskPrice1, BidVolume1, AskVolume1, "
		 * +
		 * "BidPrice2, AskPrice2, BidVolume2, AskVolume2, BidPrice3, AskPrice3, "
		 * + "BidVolume3, AskVolume3 from " +
		 * "(select TradingTime, ContractId, PreSettlementPrice, " +
		 * "CurrSettlementPrice, CurrOpenPrice, Holdings, LatestPrice, Volume, "
		 * +
		 * "TopPrice, BottomPrice, BidPrice1, AskPrice1, BidVolume1, AskVolume1, "
		 * +
		 * "BidPrice2, AskPrice2, BidVolume2, AskVolume2, BidPrice3, AskPrice3, "
		 * +
		 * "BidVolume3, AskVolume3 from xcube.futures_quotation order by tradingtime<=now(),"
		 * + "tradingtime desc) as a group by contractid;";
		 **/

		// create a view to improve the efficiency
		String sourceSql = "select * from (select TradingTime, ContractId, PreSettlementPrice, "
				+ "CurrSettlementPrice, CurrOpenPrice, Holdings, LatestPrice, Volume, "
				+ "TopPrice, BottomPrice, BidPrice1, AskPrice1, BidVolume1, AskVolume1, "
				+ "BidPrice2, AskPrice2, BidVolume2, AskVolume2, BidPrice3, AskPrice3, "
				+ "BidVolume3, AskVolume3 from xcube.futures_quotation as a "
				+ "where TradingTime=(select TradingTime from xcube.latest_futures_tradingtime "
				+ "where a.ContractId=contractid)) as b group by contractid";

		String ticker = null; // 期货代码
		float currentPrice = 0; // 现价
		float hightAndLow = 0; // 涨跌
		float hightAndLowRange = 0; // 涨跌幅
		float fiveUpdown = 0; // 5日涨跌幅
		float yearToDate = 0; // 年初至今
		Timestamp datetime = null; // 时间
		float yesterdaySettle = 0; // 昨结
		float openPrice = 0; // 开盘
		float nowHand = 0; // 现手
		float volumnCount = 0; // 总手
		float highPrice = 0; // 最高价
		float lowPrice = 0; // 最低价
		float loadingUp = 0; // 增仓
		float position = 0; // 持仓
		float volume = 0; // 成交量
		float estimationSettle = 0; // 估结算
		float outerDisk = 0; // 外盘
		float disk = 0; // 内盘
		float tradingSentiment = 0; // 交易情绪
		float windVane = 0; // 大众舆情
		float massOfPublicOpinion = 0; // 大众舆情

		if (conn == null) {
			System.out.println("reconstruct conn...");
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			System.out.println("no resultSet...");
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
			// try {
			// System.out.println(resultSet.wasNull());
			// } catch (SQLException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
		}
		PreparedStatement prestmt = null;
		try {
			// conn.prepareStatement(truncateSql).execute();

			prestmt = conn.prepareStatement(targetSql);

			while (resultSet.next()) {
				ticker = resultSet.getString("ContractId");
				datetime = resultSet.getTimestamp("TradingTime");
				currentPrice = resultSet.getFloat("LatestPrice");
				yesterdaySettle = resultSet.getFloat("PreSettlementPrice");
				hightAndLow = calHightAndLow(currentPrice, yesterdaySettle);
				hightAndLowRange = calHightAndLowRange(currentPrice,
						yesterdaySettle);
				// create a table to store the preSettlementPrice
				fiveUpdown = calFiveUpdown(ticker, currentPrice, datetime);
				yearToDate = calYearToDate(ticker, currentPrice, datetime);
				openPrice = resultSet.getFloat("CurrOpenPrice");
				// volume in market_quotation means cumvolume
				volumnCount = resultSet.getFloat("Volume");
				nowHand = calNowHand(ticker, datetime, volumnCount);
				highPrice = resultSet.getFloat("TopPrice");
				lowPrice = resultSet.getFloat("BottomPrice");
				position = resultSet.getFloat("Holdings");
				loadingUp = calLoadingUp(ticker, datetime, position);
				volume = nowHand;
				estimationSettle = resultSet.getFloat("CurrSettlementPrice");
				outerDisk = calOuterDisk(currentPrice, resultSet);
				disk = calDisk(currentPrice, resultSet);
				// extract from TA
				tradingSentiment = calTradingSentiment(ticker, datetime);
				windVane = calWindVane();
				massOfPublicOpinion = calMassOfPublicOpinion();

				System.out.println(resultSet.getRow() + " >>> " + ticker + ","
						+ datetime + "," + currentPrice + "," + yesterdaySettle
						+ "," + hightAndLow + "," + hightAndLowRange + ","
						+ fiveUpdown + "," + yearToDate + "," + openPrice + ","
						+ volumnCount + "," + nowHand + "," + highPrice + ","
						+ lowPrice + "," + position + "," + loadingUp + ","
						+ volume + "," + estimationSettle + "," + outerDisk
						+ "," + disk + "," + tradingSentiment + "," + windVane
						+ "," + massOfPublicOpinion);

				prestmt.setString(FuturesDetails.Ticker.ordinal() + 1, ticker);
				prestmt.setFloat(FuturesDetails.CurrentPrice.ordinal() + 1,
						currentPrice);
				prestmt.setFloat(FuturesDetails.HightAndLow.ordinal() + 1,
						hightAndLow);
				prestmt.setFloat(FuturesDetails.HightAndLowRange.ordinal() + 1,
						hightAndLowRange);
				prestmt.setFloat(FuturesDetails.Fiveupdown.ordinal() + 1,
						fiveUpdown);
				prestmt.setFloat(FuturesDetails.YearToDate.ordinal() + 1,
						yearToDate);
				prestmt.setTimestamp(FuturesDetails.Datetime.ordinal() + 1,
						datetime);
				prestmt.setFloat(FuturesDetails.YesterdaySettle.ordinal() + 1,
						yesterdaySettle);
				prestmt.setFloat(FuturesDetails.OpenPrice.ordinal() + 1,
						openPrice);
				prestmt.setFloat(FuturesDetails.NowHand.ordinal() + 1, nowHand);
				prestmt.setFloat(FuturesDetails.VolumnCount.ordinal() + 1,
						volumnCount);
				prestmt.setFloat(FuturesDetails.HighPrice.ordinal() + 1,
						highPrice);
				prestmt.setFloat(FuturesDetails.LowPrice.ordinal() + 1,
						lowPrice);
				prestmt.setFloat(FuturesDetails.LoadingUp.ordinal() + 1,
						loadingUp);
				prestmt.setFloat(FuturesDetails.Position.ordinal() + 1,
						position);
				prestmt.setFloat(FuturesDetails.Volume.ordinal() + 1, volume);
				prestmt.setFloat(FuturesDetails.EstimationSettle.ordinal() + 1,
						estimationSettle);
				prestmt.setFloat(FuturesDetails.OuterDisk.ordinal() + 1,
						outerDisk);
				prestmt.setFloat(FuturesDetails.Disk.ordinal() + 1, disk);
				prestmt.setFloat(FuturesDetails.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				prestmt.setFloat(FuturesDetails.WindVane.ordinal() + 1,
						windVane);
				prestmt.setFloat(
						FuturesDetails.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				prestmt.execute();
			}

			prestmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void update(PreparedStatement prestmt) {
		// TODO Auto-generated method stub
		String ticker = null; // 期货代码
		float currentPrice = 0; // 现价
		float hightAndLow = 0; // 涨跌
		float hightAndLowRange = 0; // 涨跌幅
		float fiveUpdown = 0; // 5日涨跌幅
		float yearToDate = 0; // 年初至今
		Timestamp datetime = null; // 时间
		float yesterdaySettle = 0; // 昨结
		float openPrice = 0; // 开盘
		float nowHand = 0; // 现手
		float volumnCount = 0; // 总手
		float highPrice = 0; // 最高价
		float lowPrice = 0; // 最低价
		float loadingUp = 0; // 增仓
		float position = 0; // 持仓
		float volume = 0; // 成交量
		float estimationSettle = 0; // 估结算
		float outerDisk = 0; // 外盘
		float disk = 0; // 内盘
		float tradingSentiment = 0; // 交易情绪
		float windVane = 0; // 大众舆情
		float massOfPublicOpinion = 0; // 大众舆情

		try {
//			while (resultSet.next()) {
				ticker = resultSet.getString("ContractId");
				datetime = resultSet.getTimestamp("TradingTime");
				currentPrice = resultSet.getFloat("LatestPrice");
				yesterdaySettle = resultSet.getFloat("PreSettlementPrice");
				hightAndLow = calHightAndLow(currentPrice, yesterdaySettle);
				hightAndLowRange = calHightAndLowRange(currentPrice,
						yesterdaySettle);
				// create a table to store the preSettlementPrice
				fiveUpdown = calFiveUpdown(ticker, currentPrice, datetime);
				yearToDate = calYearToDate(ticker, currentPrice, datetime);
				openPrice = resultSet.getFloat("CurrOpenPrice");
				// volume in market_quotation means cumvolume
				volumnCount = resultSet.getFloat("Volume");
				nowHand = calNowHand(ticker, datetime, volumnCount);
				highPrice = resultSet.getFloat("TopPrice");
				lowPrice = resultSet.getFloat("BottomPrice");
				position = resultSet.getFloat("Holdings");
				loadingUp = calLoadingUp(ticker, datetime, position);
				volume = nowHand;
				estimationSettle = resultSet.getFloat("CurrSettlementPrice");
				outerDisk = calOuterDisk(currentPrice, resultSet);
				disk = calDisk(currentPrice, resultSet);
				// extract from TA
				tradingSentiment = calTradingSentiment(ticker, datetime);
				windVane = calWindVane();
				massOfPublicOpinion = calMassOfPublicOpinion();

				System.out.println(resultSet.getRow() + " FuturesDetailsUtil.update >>> " + ticker + ","
						+ datetime + "," + currentPrice + "," + yesterdaySettle
						+ "," + hightAndLow + "," + hightAndLowRange + ","
						+ fiveUpdown + "," + yearToDate + "," + openPrice + ","
						+ volumnCount + "," + nowHand + "," + highPrice + ","
						+ lowPrice + "," + position + "," + loadingUp + ","
						+ volume + "," + estimationSettle + "," + outerDisk
						+ "," + disk + "," + tradingSentiment + "," + windVane
						+ "," + massOfPublicOpinion);

				prestmt.setString(FuturesDetails.Ticker.ordinal() + 1, ticker);
				prestmt.setFloat(FuturesDetails.CurrentPrice.ordinal() + 1,
						currentPrice);
				prestmt.setFloat(FuturesDetails.HightAndLow.ordinal() + 1,
						hightAndLow);
				prestmt.setFloat(FuturesDetails.HightAndLowRange.ordinal() + 1,
						hightAndLowRange);
				prestmt.setFloat(FuturesDetails.Fiveupdown.ordinal() + 1,
						fiveUpdown);
				prestmt.setFloat(FuturesDetails.YearToDate.ordinal() + 1,
						yearToDate);
				prestmt.setTimestamp(FuturesDetails.Datetime.ordinal() + 1,
						datetime);
				prestmt.setFloat(FuturesDetails.YesterdaySettle.ordinal() + 1,
						yesterdaySettle);
				prestmt.setFloat(FuturesDetails.OpenPrice.ordinal() + 1,
						openPrice);
				prestmt.setFloat(FuturesDetails.NowHand.ordinal() + 1, nowHand);
				prestmt.setFloat(FuturesDetails.VolumnCount.ordinal() + 1,
						volumnCount);
				prestmt.setFloat(FuturesDetails.HighPrice.ordinal() + 1,
						highPrice);
				prestmt.setFloat(FuturesDetails.LowPrice.ordinal() + 1,
						lowPrice);
				prestmt.setFloat(FuturesDetails.LoadingUp.ordinal() + 1,
						loadingUp);
				prestmt.setFloat(FuturesDetails.Position.ordinal() + 1,
						position);
				prestmt.setFloat(FuturesDetails.Volume.ordinal() + 1, volume);
				prestmt.setFloat(FuturesDetails.EstimationSettle.ordinal() + 1,
						estimationSettle);
				prestmt.setFloat(FuturesDetails.OuterDisk.ordinal() + 1,
						outerDisk);
				prestmt.setFloat(FuturesDetails.Disk.ordinal() + 1, disk);
				prestmt.setFloat(FuturesDetails.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				prestmt.setFloat(FuturesDetails.WindVane.ordinal() + 1,
						windVane);
				prestmt.setFloat(
						FuturesDetails.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				prestmt.execute();
//			}

			// prestmt.close();
			// conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * following private methods are utility functions.
	 */

	float calNowHand(String ticker, Timestamp datetime,
			float curVolumnCount) {
		// TODO Auto-generated method stub
		float preCumVolume = new RawDataAccess(conn).getPrevCumVolume(ticker,
				datetime, "futures");
		return curVolumnCount - preCumVolume;
	}

	// to be completed
	float calMassOfPublicOpinion() {
		// TODO Auto-generated method stub
		return 0;
	}

	// to be completed
	float calWindVane() {
		// TODO Auto-generated method stub
		return 0;
	}

	float calTradingSentiment(String ticker, Timestamp datetime) {
		float tradingSentiment = new TradingSentimentAccess(conn).getTradingSentiment(
				ticker, datetime);
		return tradingSentiment;

		// return 0;
	}

	float calDisk(float currentPrice, ResultSet resultSet) {
		// TODO Auto-generated method stub
		int diskAmount = 0;
		if (resultSet != null) {
			try {
				// while (resultSet.next()) {
				float bidPrice1 = resultSet.getFloat("BidPrice1");
				float bidPrice2 = resultSet.getFloat("BidPrice2");
				float bidPrice3 = resultSet.getFloat("BidPrice3");
				if (currentPrice <= bidPrice1) {
					diskAmount += resultSet.getInt("BidVolume1");
				}
				if (currentPrice <= bidPrice2) {
					diskAmount += resultSet.getInt("BidVolume2");
				}
				if (currentPrice <= bidPrice3) {
					diskAmount += resultSet.getInt("BidVolume3");
				}
				// }
				return diskAmount;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return diskAmount;
	}

	float calOuterDisk(float currentPrice, ResultSet resultSet) {
		// TODO Auto-generated method stub
		int outerDiskAmount = 0;
		if (resultSet != null) {
			try {
				float askPrice1 = resultSet.getFloat("AskPrice1");
				float askPrice2 = resultSet.getFloat("AskPrice2");
				float askPrice3 = resultSet.getFloat("AskPrice3");
				if (currentPrice >= askPrice1) {
					outerDiskAmount += resultSet.getInt("AskVolume1");
				}
				if (currentPrice >= askPrice2) {
					outerDiskAmount += resultSet.getInt("AskVolume2");
				}
				if (currentPrice >= askPrice3) {
					outerDiskAmount += resultSet.getInt("AskVolume3");
				}

				return outerDiskAmount;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return outerDiskAmount;
	}

	float calLoadingUp(String ticker, Timestamp datetime,
			float curHoldings) {
		// TODO Auto-generated method stub
		float prevHoldings = new RawDataAccess(conn).getPrevHoldings(ticker,
				datetime, "futures");
		return curHoldings - prevHoldings;
	}

	float calYearToDate(String ticker, float currentPrice,
			Timestamp datetime) {
		if (firstYearToDateOperation) {
			Date date = new Date(datetime.getTime());
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
			yearToDatePrice = new RawDataAccess(conn).getNDaysBeforePrice(
					ticker, date, dayOfYear - 1);
			firstYearToDateOperation = false;
		}
		System.out.println("FuturesDetailsUtil.calyeartodate >>> " + yearToDatePrice);
		if (yearToDatePrice != 0 && yearToDatePrice != Float.MIN_NORMAL) {
			return (currentPrice - yearToDatePrice) / yearToDatePrice;
		} else {
			return Float.MAX_VALUE;
		}
	}

	float calFiveUpdown(String ticker, float currentPrice,
			Timestamp datetime) {
		// TODO Auto-generated method stub
		if (firstFiveUpdownOperation) {
			RawDataAccess rawDataAccess = new RawDataAccess(conn);
			Date date = new Date(datetime.getTime());
			fiveUpdownPrice = rawDataAccess
					.getNDaysBeforePrice(ticker, date, 5);
			firstFiveUpdownOperation = false;
		}
		if (fiveUpdownPrice != 0 && fiveUpdownPrice != Float.MIN_NORMAL) {
			return (currentPrice - fiveUpdownPrice) / fiveUpdownPrice;
		} else {
			return Float.MAX_VALUE;
		}
	}

	float calHightAndLowRange(float currentPrice, float yesterdaySettle) {
		// TODO Auto-generated method stub
		if (yesterdaySettle != 0) {
			return (currentPrice - yesterdaySettle) / yesterdaySettle;
		} else {
			return Float.MAX_VALUE;
		}
	}

	float calHightAndLow(float currentPrice, float yesterdaySettle) {
		// TODO Auto-generated method stub
		return currentPrice - yesterdaySettle;
	}

}
