package com.rongdata.dbUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import com.mysql.jdbc.Connection;

enum DebtDetails {
	Ticker, CurrentPrice, HightAndLow, HightAndLowRange, Fiveupdown, YearToDate, Datetime, YesterdaySettle, OpenPrice, NowHand, VolumnCount, HighPrice, LowPrice, LoadingUp, Position, Volume, Turnover, EstimationSettle, OuterDisk, Disk, TradingSentiment, WindVane, MassOfPublicOpinion;
}

public class DebtDetailsUtil {

	Connection conn = null;
	ResultSet resultSet = null;

	public DebtDetailsUtil() {
		// TODO Auto-generated constructor stub
	}

	public DebtDetailsUtil(Connection conn) {
		this.conn = conn;
	}

	public DebtDetailsUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public DebtDetailsUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	/**
	 * for TF,T
	 */
	public void insertAll() {
		String targetSql = "replace into xcube.com_debt_details(Ticker, CurrentPrice, "
				+ "HightAndLow, HightAndLowRange, 5updown, YearToDate, Datetime, "
				+ "YesterdaySettle, OpenPrice, NowHand, VolumnCount, HighPrice, LowPrice, "
				+ "LoadingUp, Position, Volume, EstimationSettle, OuterDisk, Disk, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) value(?, ?, ?, "
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		// String sourceSql =
		// "select TradingTime, ContractId, PreSettlementPrice, "
		// +
		// "CurrSettlementPrice, CurrOpenPrice, Holdings, LatestPrice, Volume, "
		// +
		// "TopPrice, BottomPrice, BidPrice1, AskPrice1, BidVolume1, AskVolume1, "
		// +
		// "BidPrice2, AskPrice2, BidVolume2, AskVolume2, BidPrice3, AskPrice3, "
		// +
		// "BidVolume3, AskVolume3 from (select * from xcube.market_quotation "
		// +
		// "where contractid rlike '^(TF|T)[0-9].*' order by tradingtime<=now(),"
		// + "tradingtime desc) as a group by contractid;";

		String sourceSql = "select * from (select TradingTime, ContractId, PreSettlementPrice, "
				+ "CurrSettlementPrice, CurrOpenPrice, PreHoldings, Holdings, LatestPrice, Volume, "
				+ "TopPrice, BottomPrice, BidPrice1, AskPrice1, BidVolume1, AskVolume1, "
				+ "BidPrice2, AskPrice2, BidVolume2, AskVolume2, BidPrice3, AskPrice3, "
				+ "BidVolume3, AskVolume3 from xcube.debt_quotation as a "
				+ "where TradingTime=(select TradingTime from xcube.latest_debt_tradingtime "
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
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}
		PreparedStatement prestmt = null;
		try {
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
				estimationSettle = resultSet.getFloat("CurrSettlementPRice");
				outerDisk = calOuterDisk(currentPrice, resultSet);
				disk = calDisk(currentPrice, resultSet);
				// extract from TA
				tradingSentiment = calTradingSentiment(ticker, datetime);
				windVane = calWindVane();
				massOfPublicOpinion = calMassOfPublicOpinion();

				prestmt.setString(DebtDetails.Ticker.ordinal() + 1, ticker);
				prestmt.setFloat(DebtDetails.CurrentPrice.ordinal() + 1,
						currentPrice);
				prestmt.setFloat(DebtDetails.HightAndLow.ordinal() + 1,
						hightAndLow);
				prestmt.setFloat(DebtDetails.HightAndLowRange.ordinal() + 1,
						hightAndLowRange);
				prestmt.setFloat(DebtDetails.Fiveupdown.ordinal() + 1,
						fiveUpdown);
				prestmt.setFloat(DebtDetails.YearToDate.ordinal() + 1,
						yearToDate);
				prestmt.setTimestamp(DebtDetails.Datetime.ordinal() + 1,
						datetime);
				prestmt.setFloat(DebtDetails.YesterdaySettle.ordinal() + 1,
						yesterdaySettle);
				prestmt.setFloat(DebtDetails.OpenPrice.ordinal() + 1, openPrice);
				prestmt.setFloat(DebtDetails.NowHand.ordinal() + 1, nowHand);
				prestmt.setFloat(DebtDetails.VolumnCount.ordinal() + 1,
						volumnCount);
				prestmt.setFloat(DebtDetails.HighPrice.ordinal() + 1, highPrice);
				prestmt.setFloat(DebtDetails.LowPrice.ordinal() + 1, lowPrice);
				prestmt.setFloat(DebtDetails.LoadingUp.ordinal() + 1, loadingUp);
				prestmt.setFloat(DebtDetails.Position.ordinal() + 1, position);
				prestmt.setFloat(DebtDetails.Volume.ordinal() + 1, volume);
				prestmt.setFloat(DebtDetails.EstimationSettle.ordinal() + 1,
						estimationSettle);
				prestmt.setFloat(DebtDetails.OuterDisk.ordinal() + 1, outerDisk);
				prestmt.setFloat(DebtDetails.Disk.ordinal() + 1, disk);
				prestmt.setFloat(DebtDetails.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				prestmt.setFloat(DebtDetails.WindVane.ordinal() + 1, windVane);
				prestmt.setFloat(DebtDetails.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				prestmt.execute();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * calNowHand Float Version
	 * 
	 * @param ticker
	 * @param datetime
	 * @param curVolumnCount
	 * @return
	 */
	float calNowHand(String ticker, Timestamp datetime, float curVolumnCount) {
		// TODO Auto-generated method stub
		float preCumVolume = new RawDataAccess(conn).getPrevCumVolume(ticker,
				datetime, "debt").floatValue();
		return curVolumnCount - preCumVolume;
	}

	/**
	 * calNowHand BigInteger Version
	 * 
	 * @param ticker
	 * @param datetime
	 * @param volumnCount
	 * @return
	 */
	public BigInteger calNowHand(String ticker, Timestamp datetime,
			BigInteger curVolumnCount) {
		// TODO Auto-generated method stub
		BigInteger preCumVolume = new RawDataAccess(conn).getPrevCumVolume(
				ticker, datetime, "debt");
		BigInteger nowHand = curVolumnCount.subtract(preCumVolume);
		return nowHand;
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
		// TODO Auto-generated method stub
		float tradingSentiment = new TradingSentimentAccess(conn)
				.getTradingSentiment(ticker, datetime);
		return tradingSentiment;
	}

	/**
	 * calDisk Float Version
	 * 
	 * @param currentPrice
	 * @param resultSet
	 * @return
	 */
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

	/**
	 * calDisk BigInteger Version
	 * 
	 * @param currentPrice
	 * @param resultSet2
	 * @return
	 */
	public BigInteger calDisk(BigDecimal currentPrice, ResultSet resultSet) {
		// TODO Auto-generated method stub
		BigInteger diskAmount = BigInteger.ZERO;
		if (resultSet != null) {
			try {
				// while (resultSet.next()) {
				BigDecimal bidPrice1 = resultSet.getBigDecimal("BidPrice1");
				BigDecimal bidPrice2 = resultSet.getBigDecimal("BidPrice2");
				BigDecimal bidPrice3 = resultSet.getBigDecimal("BidPrice3");
				int resultForFirst = currentPrice.compareTo(bidPrice1);
				int resultForSecond = currentPrice.compareTo(bidPrice2);
				int resultForThird = currentPrice.compareTo(bidPrice3);

				if (resultForFirst <= 0) {
					diskAmount = diskAmount.add(BigInteger.valueOf(resultSet
							.getLong("BidVolume1")));
				}
				if (resultForSecond <= 0) {
					diskAmount = diskAmount.add(BigInteger.valueOf(resultSet
							.getLong("BidVolume2")));
				}
				if (resultForThird <= 0) {
					diskAmount = diskAmount.add(BigInteger.valueOf(resultSet
							.getLong("BidVolume3")));
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

	/**
	 * calOuterDisk Float Version
	 * 
	 * @param currentPrice
	 * @param resultSet
	 * @return
	 */
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

	/**
	 * calOuterDisk BigInteger Version
	 * 
	 * @param currentPrice
	 * @param resultSet2
	 * @return
	 */
	public BigInteger calOuterDisk(BigDecimal currentPrice, ResultSet resultSet) {
		// TODO Auto-generated method stub
		BigInteger outerDiskAmount = BigInteger.ZERO;
		if (resultSet != null) {
			try {
				BigDecimal askPrice1 = resultSet.getBigDecimal("AskPrice1");
				BigDecimal askPrice2 = resultSet.getBigDecimal("AskPrice2");
				BigDecimal askPrice3 = resultSet.getBigDecimal("AskPrice3");
				int resultForFirst = currentPrice.compareTo(askPrice1);
				int resultForSecond = currentPrice.compareTo(askPrice2);
				int resultForThird = currentPrice.compareTo(askPrice3);
				if (resultForFirst >= 0) {
					outerDiskAmount = outerDiskAmount.add(BigInteger
							.valueOf(resultSet.getLong("AskVolume1")));
				}
				if (resultForSecond >= 0) {
					outerDiskAmount = outerDiskAmount.add(BigInteger
							.valueOf(resultSet.getLong("AskVolume2")));
				}
				if (resultForThird >= 0) {
					outerDiskAmount = outerDiskAmount.add(BigInteger
							.valueOf(resultSet.getLong("AskVolume3")));
				}

				return outerDiskAmount;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return outerDiskAmount;
	}

	/**
	 * calLoadingUp Float Version
	 * 
	 * @param ticker
	 * @param datetime
	 * @param curHoldings
	 * @return
	 */
	float calLoadingUp(String ticker, Timestamp datetime, float curHoldings) {
		// TODO Auto-generated method stub
		float prevHoldings = new RawDataAccess(conn).getPrevHoldings(ticker,
				datetime, "debt").floatValue();
		return curHoldings - prevHoldings;
	}

	/**
	 * calLoadingUp BigInteger Version
	 * 
	 * @param ticker
	 * @param datetime
	 * @param position
	 * @return
	 */
	public BigInteger calLoadingUp(String ticker, Timestamp datetime,
			BigInteger curHoldings) {
		// TODO Auto-generated method stub
		BigInteger prevHoldings = new RawDataAccess(conn).getPrevHoldings(
				ticker, datetime, "debt");
		return curHoldings.subtract(prevHoldings);
	}

	/**
	 * calYearToDate Float Version
	 * 
	 * @param ticker
	 * @param currentPrice
	 * @param datetime
	 * @return
	 */
	float calYearToDate(String ticker, float currentPrice, Timestamp datetime) {
		Date date = new Date(datetime.getTime());
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
		float yearToDatePrice = new RawDataAccess(conn).getNDaysBeforePrice(
				ticker, date, dayOfYear - 1).floatValue();
		// TODO Auto-generated method stub
		if (yearToDatePrice != 0 && yearToDatePrice != Float.MIN_NORMAL) {
			return (currentPrice - yearToDatePrice) / yearToDatePrice;
		} else {
			return 0;
		}
	}

	/**
	 * calYearToDate BigDecimal Version
	 * 
	 * @param ticker
	 * @param currentPrice
	 * @param datetime
	 * @return
	 */
	public float calYearToDate(String ticker, BigDecimal currentPrice,
			Timestamp datetime) {
		// TODO Auto-generated method stub
		Date date = new Date(datetime.getTime());
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
		BigDecimal yearToDatePrice = new RawDataAccess(conn)
				.getNDaysBeforePrice(ticker, date, dayOfYear - 1);
		// TODO Auto-generated method stub
		if (yearToDatePrice != null && currentPrice != null
				&& yearToDatePrice.compareTo(BigDecimal.ZERO) != 0) {
			BigDecimal yearToDate = currentPrice.subtract(yearToDatePrice)
					.divide(yearToDatePrice, 2, BigDecimal.ROUND_HALF_DOWN);
			return yearToDate.floatValue();
		} else {
			return 0;
		}
	}
	
	public float calYearToDate(BigDecimal currentPrice,BigDecimal yearToDatePrice) {
		// TODO Auto-generated method stub
		if (yearToDatePrice != null && currentPrice != null
				&& yearToDatePrice.compareTo(BigDecimal.ZERO) != 0) {
			BigDecimal yearToDate = currentPrice.subtract(yearToDatePrice)
					.divide(yearToDatePrice, 2, BigDecimal.ROUND_HALF_DOWN);
			return yearToDate.floatValue();
		} else {
			return 0;
		}
	}

	/**
	 * calFiveUpdown Float Version
	 * 
	 * @param ticker
	 * @param currentPrice
	 * @param datetime
	 * @return
	 */
	float calFiveUpdown(String ticker, float currentPrice, Timestamp datetime) {
		// TODO Auto-generated method stub
		RawDataAccess rawDataAccess = new RawDataAccess(conn);
		Date date = new Date(datetime.getTime());
		float fiveUpdownPrice = rawDataAccess.getNDaysBeforePrice(ticker, date,
				5).floatValue();
		if (currentPrice != 0) {
			return (currentPrice - fiveUpdownPrice) / currentPrice;
		} else {
			return 0;
		}
	}

	/**
	 * calFiveUpdown BigDecimal Version
	 * 
	 * @param ticker
	 * @param currentPrice
	 * @param datetime
	 * @return
	 */
	public float calFiveUpdown(String ticker, BigDecimal currentPrice,
			Timestamp datetime) {
		// TODO Auto-generated method stub
		RawDataAccess rawDataAccess = new RawDataAccess(conn);
		Date date = new Date(datetime.getTime());
		BigDecimal fiveUpdownPrice = rawDataAccess.getNDaysBeforePrice(ticker,
				date, 5);
		if (fiveUpdownPrice != null && currentPrice != null
				&& fiveUpdownPrice.compareTo(BigDecimal.ZERO) != 0) {
			BigDecimal fiveUpdown = currentPrice.subtract(fiveUpdownPrice)
					.divide(fiveUpdownPrice, 2, BigDecimal.ROUND_HALF_DOWN);
			return fiveUpdown.floatValue();
		} else {
			return 0;
		}
	}

	public float calFiveUpdown(BigDecimal currentPrice,
			BigDecimal fiveUpdownPrice) {
		// TODO Auto-generated method stub
		if (fiveUpdownPrice != null && currentPrice != null
				&& fiveUpdownPrice.compareTo(BigDecimal.ZERO) != 0) {
			BigDecimal fiveUpdown = currentPrice.subtract(fiveUpdownPrice)
					.divide(fiveUpdownPrice, 2, BigDecimal.ROUND_HALF_DOWN);
			return fiveUpdown.floatValue();
		} else {
			return 0;
		}
	}

	/**
	 * calHightAndLowRange Float Version
	 * 
	 * @param currentPrice
	 * @param yesterdaySettle
	 * @return
	 */
	float calHightAndLowRange(float currentPrice, float yesterdaySettle) {
		// TODO Auto-generated method stub
		if (yesterdaySettle != 0) {
			return (currentPrice - yesterdaySettle) / yesterdaySettle;
		} else {
			return Float.MAX_VALUE;
		}
	}

	/**
	 * calHightAndLowRange BigDecimal Version
	 * 
	 * @param currentPrice
	 * @param yesterdaySettle
	 * @return
	 */
	public float calHightAndLowRange(BigDecimal currentPrice,
			BigDecimal yesterdaySettle) {
		// TODO Auto-generated method stub
		if (currentPrice == null || yesterdaySettle == null) {
			System.out
					.println("DebtDetailsUtil.calHightAndLowRange >>> parameters null");
			return 0;
		}
		if (yesterdaySettle.compareTo(BigDecimal.ZERO) != 0) {
			BigDecimal hightAndLowRange = currentPrice
					.subtract(yesterdaySettle).divide(yesterdaySettle, 2,
							BigDecimal.ROUND_HALF_DOWN);
			return hightAndLowRange.floatValue();
		} else {
			return 0;
		}
	}

	/**
	 * calHightAndLow Float Version
	 * 
	 * @param currentPrice
	 * @param yesterdaySettle
	 * @return
	 */
	float calHightAndLow(float currentPrice, float yesterdaySettle) {
		// TODO Auto-generated method stub
		return currentPrice - yesterdaySettle;
	}

	/**
	 * calHightAndLow BigDecimal Version
	 * 
	 * @param currentPrice
	 * @param yesterdaySettle
	 * @return
	 */
	public float calHightAndLow(BigDecimal currentPrice,
			BigDecimal yesterdaySettle) {
		// TODO Auto-generated method stub
		if (currentPrice == null || yesterdaySettle == null) {
			System.out
					.println("DebtDetailsUtil.calHightAndLow >>> parameters null");
			return 0;
		}
		BigDecimal hightAndLow = currentPrice.subtract(yesterdaySettle)
				.setScale(2, BigDecimal.ROUND_HALF_DOWN);
		return hightAndLow.floatValue();
	}

}
