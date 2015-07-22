package com.rongdata.dbUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import com.mysql.jdbc.Connection;

enum SingleTickerDetails {
	StockCode, Datetime, CurrentPrice, HightAndLow, HightAndLowRange, YesterdayReceived, TodayOpenPrice, Volume, Turnover, TurnoverRate, HightPrice, LowPrice, AveragePrice, PriceEarningsRatio, VolumeCount, VolumeRatio, PriceToBook, TradingSentiment, WindVane, MassOfPublicOpinion, TotalMarketValue, CirculationValue;
}

public class SingleTickerDetailsUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;
	private boolean firstVolumeRatioOperation = true;
	private BigInteger pastFiveDaysCumVolume = BigInteger.ZERO;

	public SingleTickerDetailsUtil() {
		// TODO Auto-generated constructor stub
	}

	public SingleTickerDetailsUtil(Connection conn) {
		this.conn = conn;
	}

	public SingleTickerDetailsUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public SingleTickerDetailsUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	/**
	 * for SH,SZ
	 */
	public void insertAll() {
		String targetSql = "insert ignore into xcube.com_single_ticker_details(StockCode, "
				+ "Datetime, CurrentPrice, HightAndLow, HightAndLowRange, YesterdayReceived, "
				+ "TodayOpenPrice, Volume, Turnover, TurnoverRate, HightPrice, LowPrice, "
				+ "AveragePrice, PriceEarningsRatio, VolumeCount, VolumeRatio, PriceToBook, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion, TotalMarketValue, "
				+ "CirculationValue) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ "?, ?, ?, ?, ?);";
		String sourceSql = "select TradingTime, ContractId, PreClosePrice, CurrOpenPrice, "
				+ "Holdings, LatestPrice, Volume, Turnover, AveragePrice, TopPrice, BottomPrice "
				+ "from (select TradingTime, ContractId, PreClosePrice, CurrOpenPrice, "
				+ "Holdings, LatestPrice, Volume, Turnover, AveragePrice, TopPrice, BottomPrice "
				+ "from xcube.market_quotation "
				+ "where contractid rlike '^S.*' order by tradingtime<=now(),"
				+ "tradingtime desc) as a group by contractid;";

		String stockCode = null; // 证券代码
		Timestamp datetime = null; // 时间
		float currentPrice = 0; // 当前价格
		float hightAndLow = 0; // 涨跌
		float hightAndLowRange = 0; // 涨跌幅
		float yesterdayReceived = 0; // 昨收
		float todayOpenPrice = 0; // 今开
		float volume = 0; // 成交量
		float turnover = 0; // 成交额
		float turnoverRate = 0; // 换手率
		float hightPrice = 0; // 最高价
		float lowPrice = 0; // 最低价
		float averagePrice = 0; // 平均价
		float priceEarningsRatio = 0; // 市盈率
		float volumeCount = 0; // 总手
		float volumeRatio = 0; // 量比
		float priceToBook = 0; // 市净率
		float tradingSentiment = 0; // 交易情绪
		float windVane = 0; // 风向标
		float massOfPublicOpinion = 0; // 大众舆情
		float totalMarketValue = 0; // 总市值
		float circulationValue = 0; // 流通值

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}

		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);

			while (resultSet.next()) {
				stockCode = resultSet.getString("ContractId");
				datetime = resultSet.getTimestamp("TradingTime");
				currentPrice = resultSet.getFloat("LatestPrice");
				yesterdayReceived = resultSet.getFloat("PreClosePrice");
				hightAndLow = calHightAndLow(currentPrice, yesterdayReceived);
				hightAndLowRange = calHightAndLowRange(currentPrice,
						yesterdayReceived);
				todayOpenPrice = resultSet.getFloat("CurrOpenPrice");
				volume = resultSet.getInt("Volume");
				turnover = resultSet.getFloat("TurnOver");
				turnoverRate = calTurnoverRate(volume);
				hightPrice = resultSet.getFloat("TopPrice");
				lowPrice = resultSet.getFloat("BottomPrice");
				averagePrice = resultSet.getFloat("AveragePrice");
				priceEarningsRatio = calPriceEarningsRatio(currentPrice);
				volumeCount = volume;
				volumeRatio = calVolumeRatio(stockCode, datetime, volume);
				priceToBook = calPriceToBook(currentPrice);
				tradingSentiment = calTradingSentiment(stockCode, datetime);
				windVane = calWindVane();
				massOfPublicOpinion = calMassOfPublicOpinion();
				totalMarketValue = calTotalMarketValue(currentPrice);
				circulationValue = calCirculationValue(currentPrice);
				System.out.println("singletickerdetailsutil.insertall >>> "
						+ volumeRatio);

				prestmt.setString(SingleTickerDetails.StockCode.ordinal() + 1,
						stockCode);
				prestmt.setTimestamp(
						SingleTickerDetails.Datetime.ordinal() + 1, datetime);
				prestmt.setFloat(
						SingleTickerDetails.CurrentPrice.ordinal() + 1,
						currentPrice);
				prestmt.setFloat(SingleTickerDetails.HightAndLow.ordinal() + 1,
						hightAndLow);
				prestmt.setFloat(
						SingleTickerDetails.HightAndLowRange.ordinal() + 1,
						hightAndLowRange);
				prestmt.setFloat(
						SingleTickerDetails.YesterdayReceived.ordinal() + 1,
						yesterdayReceived);
				prestmt.setFloat(
						SingleTickerDetails.TodayOpenPrice.ordinal() + 1,
						todayOpenPrice);
				prestmt.setFloat(SingleTickerDetails.Volume.ordinal() + 1,
						volume);
				prestmt.setFloat(SingleTickerDetails.Turnover.ordinal() + 1,
						turnover);
				prestmt.setFloat(
						SingleTickerDetails.TurnoverRate.ordinal() + 1,
						turnoverRate);
				prestmt.setFloat(SingleTickerDetails.HightPrice.ordinal() + 1,
						hightPrice);
				prestmt.setFloat(SingleTickerDetails.LowPrice.ordinal() + 1,
						lowPrice);
				prestmt.setFloat(
						SingleTickerDetails.AveragePrice.ordinal() + 1,
						averagePrice);
				prestmt.setFloat(
						SingleTickerDetails.PriceEarningsRatio.ordinal() + 1,
						priceEarningsRatio);
				prestmt.setFloat(SingleTickerDetails.VolumeCount.ordinal() + 1,
						volumeCount);
				prestmt.setFloat(SingleTickerDetails.VolumeRatio.ordinal() + 1,
						volumeRatio);
				prestmt.setFloat(SingleTickerDetails.PriceToBook.ordinal() + 1,
						priceToBook);
				prestmt.setFloat(
						SingleTickerDetails.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				prestmt.setFloat(SingleTickerDetails.WindVane.ordinal() + 1,
						windVane);
				prestmt.setFloat(
						SingleTickerDetails.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);
				prestmt.setFloat(
						SingleTickerDetails.TotalMarketValue.ordinal() + 1,
						totalMarketValue);
				prestmt.setFloat(
						SingleTickerDetails.CirculationValue.ordinal() + 1,
						circulationValue);

				prestmt.execute();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * calHightAndLow Float Version
	 * 
	 * @param currentPrice
	 * @param yesterdayReceived
	 * @return
	 */
	float calHightAndLow(float currentPrice, float yesterdayReceived) {
		// TODO Auto-generated method stub
		return currentPrice - yesterdayReceived;
	}

	/**
	 * calHightAndLow BigDecimal Version
	 * 
	 * @param currentPrice
	 * @param yesterdayReceived
	 * @return
	 */
	public float calHightAndLow(BigDecimal currentPrice,
			BigDecimal yesterdayReceived) {
		// TODO Auto-generated method stub
		if (currentPrice == null || yesterdayReceived == null) {
			System.out
					.println("SingleTickerDetailsUtil.calHightAndLow >>> parameters null");
			return 0;
		}

		BigDecimal hightAndLow = currentPrice.subtract(yesterdayReceived)
				.setScale(2, BigDecimal.ROUND_HALF_DOWN);
		return hightAndLow.floatValue();
	}

	/**
	 * calHightAndLowRange Float Version
	 * 
	 * @param currentPrice
	 * @param yesterdayReceived
	 * @return
	 */
	float calHightAndLowRange(float currentPrice, float yesterdayReceived) {
		// TODO Auto-generated method stub
		if (yesterdayReceived != 0) {
			return (currentPrice - yesterdayReceived) / yesterdayReceived;
		} else {
			return Float.MAX_VALUE;
		}
	}

	/**
	 * calHightAndLowRange BigDecimal Version
	 * 
	 * @param currentPrice
	 * @param yesterdayReceived
	 * @return
	 */
	public float calHightAndLowRange(BigDecimal currentPrice,
			BigDecimal yesterdayReceived) {
		// TODO Auto-generated method stub
		if (currentPrice == null || yesterdayReceived == null) {
			System.out
					.println("SingleTickerDetailsUtil.calHightAndLowRange >>> parameters null");
		}
		if (yesterdayReceived.compareTo(BigDecimal.ZERO) != 0) {
			BigDecimal hightAndLowRange = currentPrice.subtract(
					yesterdayReceived).divide(yesterdayReceived, 2,
					BigDecimal.ROUND_HALF_DOWN);
			return hightAndLowRange.floatValue();
		} else {
			return Float.MAX_VALUE;
		}
	}

	// to be completed, lack of outstanding shares
	/**
	 * calTurnoverRate Float Version
	 * 
	 * @param volume
	 * @return
	 */
	float calTurnoverRate(float volume) {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * calTurnoverRate BigInteger Version
	 * 
	 * @param volume
	 * @return
	 */
	public float calTurnoverRate(BigInteger volume) {
		// TODO Auto-generated method stub
		return 0;
	}

	// to be completed, lack of EPS
	/**
	 * calPriceEarningsRatio Float Version
	 * 
	 * @param currentPrice
	 * @return
	 */
	float calPriceEarningsRatio(float currentPrice) {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * calPriceEarningsRatio BigDecimal Version
	 * 
	 * @param currentPrice
	 * @return
	 */
	public float calPriceEarningsRatio(BigDecimal currentPrice) {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * calVolumeRatio Float Version
	 * 
	 * @param ticker
	 * @param datetime
	 * @param curCumVolume
	 * @return
	 */
	float calVolumeRatio(String ticker, Timestamp datetime, float curCumVolume) {
		// TODO Auto-generated method stub
		int daysCount = 5;
		if (firstVolumeRatioOperation) {
			pastFiveDaysCumVolume = new RawDataAccess(conn)
					.getPastNDaysCumVolume(ticker, datetime, daysCount);
			firstVolumeRatioOperation = false;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(datetime.getTime());
		int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
		int minutes = calendar.get(Calendar.MINUTE);
		int curCumMinutes = 0;
		if (hourOfDay == 9) {
			curCumMinutes += minutes - 30;
		} else if (hourOfDay > 9 && hourOfDay < 12) {
			curCumMinutes += (hourOfDay - 9) * 60 + minutes;
		} else if (hourOfDay >= 13 && hourOfDay < 15) {
			curCumMinutes += (hourOfDay - 13 + 2) * 60 + minutes;
		}
		// System.out.println("singletickerdetailsutil.calvolumeratio >>> " +
		// pastFiveDaysCumVolume);
		if (pastFiveDaysCumVolume.floatValue() != 0
				&& pastFiveDaysCumVolume != null) {
			// System.out.println("singletickerdetailsutil.calvolumeratio >>> enter if clause, result");
			return (curCumVolume / curCumMinutes)
					/ (pastFiveDaysCumVolume.floatValue() / (5 * 240));
		}
		// return Float.MAX_VALUE;
		return 0;
	}

	/**
	 * calVolumeRatio BigInteger Version
	 * 
	 * @param ticker
	 * @param datetime
	 * @param volume
	 * @return
	 */
	public float calVolumeRatio(String ticker, Timestamp datetime,
			BigInteger curCumVolume) {
		// TODO Auto-generated method stub
		int daysCount = 5;
		if (firstVolumeRatioOperation) {
			pastFiveDaysCumVolume = new RawDataAccess(conn)
					.getPastNDaysCumVolume(ticker, datetime, daysCount);
			firstVolumeRatioOperation = false;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(datetime.getTime());
		int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
		int minutes = calendar.get(Calendar.MINUTE);
		int curCumMinutes = 0;
		if (hourOfDay == 9) {
			curCumMinutes += minutes - 30;
		} else if (hourOfDay > 9 && hourOfDay < 12) {
			curCumMinutes += (hourOfDay - 9) * 60 + minutes;
		} else if (hourOfDay >= 13 && hourOfDay < 15) {
			curCumMinutes += (hourOfDay - 13 + 2) * 60 + minutes;
		}
		// System.out.println("singletickerdetailsutil.calvolumeratio >>> " +
		// pastFiveDaysCumVolume);
		if (pastFiveDaysCumVolume.compareTo(BigInteger.ZERO) != 0
				&& pastFiveDaysCumVolume != null) {
			// System.out.println("singletickerdetailsutil.calvolumeratio >>> enter if clause, result");
			BigInteger volumeRatio = curCumVolume
					.multiply(BigInteger.valueOf(5 * 240))
					.divide(BigInteger.valueOf(curCumMinutes))
					.divide(pastFiveDaysCumVolume);
			return volumeRatio.floatValue();
		}
		// return Float.MAX_VALUE;
		return 0;
	}

	// to be completed, lack of BPS
	/**
	 * calPriceToBook Float Version
	 * @param currentPrice
	 * @return
	 */
	float calPriceToBook(float currentPrice) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/**
	 * calPriceToBook BigDecimal Version
	 * @param currentPrice
	 * @return
	 */
	public float calPriceToBook(BigDecimal currentPrice) {
		// TODO Auto-generated method stub
		return 0;
	}

	float calTradingSentiment(String stockCode, Timestamp datetime) {
		// TODO Auto-generated method stub
		float tradingSentiment = new TradingSentimentAccess(conn)
				.getTradingSentiment(stockCode, datetime);
		return tradingSentiment;
	}

	// to be completed
	float calWindVane() {
		// TODO Auto-generated method stub
		return 0;
	}

	// to be completed
	float calMassOfPublicOpinion() {
		// TODO Auto-generated method stub
		return 0;
	}

	// to be completed, lack of total shares
	/**
	 * calTotalMarketValue Float Version
	 * @param currentPrice
	 * @return
	 */
	float calTotalMarketValue(float currentPrice) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/**
	 * calTotalMarketValue BigDecimal Version
	 * @param currentPrice
	 * @return
	 */
	public BigDecimal calTotalMarketValue(BigDecimal currentPrice) {
		// TODO Auto-generated method stub
		return null;
	}

	// to be completed, lack of outstanding shares
	/**
	 * calCirculationValue Float Version
	 * @param currentPrice
	 * @return
	 */
	float calCirculationValue(float currentPrice) {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * calCirculationValue BigDecimal Version
	 * @param currentPrice
	 * @return
	 */
	public BigDecimal calCirculationValue(BigDecimal currentPrice) {
		// TODO Auto-generated method stub
		return null;
	}

}
