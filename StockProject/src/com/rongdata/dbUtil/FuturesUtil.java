package com.rongdata.dbUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

import com.mysql.jdbc.Connection;

public class FuturesUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;

	PreparedStatement detailsPrestmt = null;
	private PreparedStatement propertyPrestmt = null;
	private PreparedStatement kChartsDayPrestmt = null;
	private PreparedStatement positionPrestmt = null;
	
	private HashMap<String, ArrayList<BigDecimal>> tickerPreSettleMap = new HashMap<String, ArrayList<BigDecimal>>();

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

	public FuturesUtil() {
		// TODO Auto-generated constructor stub
	}

	public FuturesUtil(Connection conn) {
		this.conn = conn;
	}

	public FuturesUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public FuturesUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	public void operate() {
		// String sourceSql =
		// "select * from (select TradingTime, ContractId, PreSettlementPrice, "
		// +
		// "CurrSettlementPrice, CurrOpenPrice, PreHoldings, Holdings, LatestPrice, Volume, "
		// +
		// "Turnover, TopPrice, BottomPrice, BidPrice1, AskPrice1, BidVolume1, AskVolume1, "
		// +
		// "BidPrice2, AskPrice2, BidVolume2, AskVolume2, BidPrice3, AskPrice3, "
		// + "BidVolume3, AskVolume3 from xcube.futures_quotation as a "
		// +
		// "where TradingTime=(select TradingTime from xcube.latest_futures_tradingtime "
		// + "where a.ContractId=contractid)) as b group by contractid";

		String sourceSql = "select TradingTime, ContractId, PreSettlementPrice, CurrSettlementPrice, "
				+ "CurrOpenPrice, PreHoldings, Holdings, LatestPrice, Volume, Turnover, TopPrice, "
				+ "BottomPrice, BidPrice1, AskPrice1, BidVolume1, AskVolume1, BidPrice2, AskPrice2, "
				+ "BidVolume2, AskVolume2, BidPrice3, AskPrice3, BidVolume3, AskVolume3 "
				+ "from (select TradingTime, ContractId, PreSettlementPrice, CurrSettlementPrice, "
				+ "CurrOpenPrice, PreHoldings, Holdings, LatestPrice, Volume, Turnover, TopPrice, "
				+ "BottomPrice, BidPrice1, AskPrice1, BidVolume1, AskVolume1, BidPrice2, AskPrice2, "
				+ "BidVolume2, AskVolume2, BidPrice3, AskPrice3, BidVolume3, AskVolume3 "
				+ "from xcube.futures_quotation order by tradingtime desc limit 50) as a "
				+ "group by contractid;";

		String detailsSql = "replace into xcube.com_futures_details(Ticker, CurrentPrice, "
				+ "HightAndLow, HightAndLowRange, 5updown, YearToDate, Datetime, "
				+ "YesterdaySettle, OpenPrice, NowHand, VolumnCount, HighPrice, LowPrice, "
				+ "LoadingUp, Position, Volume, Turnover, EstimationSettle, OuterDisk, Disk, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) values(?, ?, ?, "
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		String propertySql = "replace into xcube.com_futures_property(Ticker, TickerName, "
				+ "ListingDate, UpdateDate, Validity, Type, SuperFutures) values(?, ?, ?, "
				+ "?, ?, ?, ?)";

		String positionSql = "replace into xcube.com_futures_position(Ticker, Datetime, "
				+ "Price, NowHand, LoadingUp, Nature) values(?, ?, ?, ?, ?, ?)";

		// update futures data into kchartsday table
		String kChartsDaySql = "replace into xcube.com_k_charts_day(Ticker, Datetime, "
				+ "OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		// parameters in FuturesDetails table
		String ticker = null; // 期货代码
		BigDecimal currentPrice = BigDecimal.ZERO; // 现价
		float hightAndLow = 0; // 涨跌
		float hightAndLowRange = 0; // 涨跌幅
		float fiveUpdown = 0; // 5日涨跌幅
		float yearToDate = 0; // 年初至今
		Timestamp datetime = null; // 时间
		BigDecimal yesterdaySettle = BigDecimal.ZERO; // 昨结
		BigDecimal openPrice = BigDecimal.ZERO; // 开盘
		BigInteger nowHand = BigInteger.ZERO; // 现手
		BigInteger volumnCount = BigInteger.ZERO; // 总手
		BigDecimal highPrice = BigDecimal.ZERO; // 最高价
		BigDecimal lowPrice = BigDecimal.ZERO; // 最低价
		BigInteger loadingUp = BigInteger.ZERO; // 增仓
		BigInteger position = BigInteger.ZERO; // 持仓
		BigInteger volume = BigInteger.ZERO; // 成交量
		BigDecimal turnover = BigDecimal.ZERO; // 成交金额
		BigDecimal estimationSettle = BigDecimal.ZERO; // 估结算
		BigInteger outerDisk = BigInteger.ZERO; // 外盘
		BigInteger disk = BigInteger.ZERO; // 内盘
		float tradingSentiment = 0; // 交易情绪
		float windVane = 0; // 大众舆情
		float massOfPublicOpinion = 0; // 大众舆情

		// parameters in FuturesProperty table
		String tickerName = ticker;
		Date listingDate = null;
		Date updateDate = null;
		int validity = 1;
		int type = 0;
		String superFutures = null;

		// parameters in FuturesPosition table
		float nature = 0;

		// parameters in kchartsday table
		BigDecimal closePrice = BigDecimal.ZERO; // 收盘价格
		BigDecimal amount = BigDecimal.ZERO; // 累计成交金额

		if (conn == null) {
			System.out.println("futuresutil.operate >>> reconstruct conn");
			conn = MysqlDBUtil.getConnection();
		}

		System.out.println("    >>> get futures result");
		RawDataAccess rawDataAccess = new RawDataAccess(conn);
		resultSet = rawDataAccess.getRawData(sourceSql);

		try {
			detailsPrestmt = conn.prepareStatement(detailsSql);
			propertyPrestmt = conn.prepareStatement(propertySql);
			kChartsDayPrestmt = conn.prepareStatement(kChartsDaySql);
			positionPrestmt = conn.prepareStatement(positionSql);

			FuturesDetailsUtil futuresDetailsUtil = new FuturesDetailsUtil(conn);
			FuturesPropertyUtil futuresPropertyUtil = new FuturesPropertyUtil(
					conn);
			FuturesPositionUtil futuresPositionUtil = new FuturesPositionUtil(
					conn);

			while (resultSet.next()) {
				// parameters in FuturesDetails table
				ticker = resultSet.getString("ContractId");
				datetime = resultSet.getTimestamp("TradingTime");
				currentPrice = resultSet.getBigDecimal("LatestPrice");
				yesterdaySettle = resultSet.getBigDecimal("PreSettlementPrice");
				hightAndLow = futuresDetailsUtil.calHightAndLow(currentPrice,
						yesterdaySettle);
				hightAndLowRange = futuresDetailsUtil.calHightAndLowRange(
						currentPrice, yesterdaySettle);
				// create a table to store the preSettlementPrice
				if (!tickerPreSettleMap.containsKey(ticker)) {
					BigDecimal fiveUpdownPrice = rawDataAccess
							.getNDaysBeforePrice(ticker,
									new Date(datetime.getTime()), 5);

					Date date = new Date(datetime.getTime());
					Calendar calendar = Calendar.getInstance();
					calendar.setTime(date);
					int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
					BigDecimal yearToDatePrice = rawDataAccess
							.getNDaysBeforePrice(tickerName, date,
									dayOfYear - 1);

					ArrayList<BigDecimal> list = new ArrayList<BigDecimal>();
					list.add(fiveUpdownPrice);
					list.add(yearToDatePrice);
					tickerPreSettleMap.put(ticker, list);
				}
				
				fiveUpdown = futuresDetailsUtil.calFiveUpdown(currentPrice,
						tickerPreSettleMap.get(ticker).get(0));
				yearToDate = futuresDetailsUtil.calYearToDate(currentPrice,
						tickerPreSettleMap.get(ticker).get(1));
				
				openPrice = resultSet.getBigDecimal("CurrOpenPrice");
				// volume in market_quotation means cumvolume
				volumnCount = BigInteger.valueOf(resultSet.getLong("Volume"));
				nowHand = futuresDetailsUtil.calNowHand(ticker, datetime,
						volumnCount);
				highPrice = resultSet.getBigDecimal("TopPrice");
				lowPrice = resultSet.getBigDecimal("BottomPrice");
				position = BigInteger.valueOf(resultSet.getLong("Holdings"));
				loadingUp = futuresDetailsUtil.calLoadingUp(ticker, datetime,
						position);
				volume = nowHand;
				estimationSettle = resultSet
						.getBigDecimal("CurrSettlementPrice");
				outerDisk = futuresDetailsUtil.calOuterDisk(currentPrice,
						resultSet);
				disk = futuresDetailsUtil.calDisk(currentPrice, resultSet);
				tradingSentiment = futuresDetailsUtil.calTradingSentiment(
						ticker, datetime);
				windVane = futuresDetailsUtil.calWindVane();
				massOfPublicOpinion = futuresDetailsUtil
						.calMassOfPublicOpinion();

				// parameters in FuturesProperty table
				tickerName = ticker;
				listingDate = futuresPropertyUtil.calListingDate(ticker);
				updateDate = futuresPropertyUtil.calUpdateDate();
				Timestamp tradingTime = resultSet.getTimestamp("TradingTime");
				validity = futuresPropertyUtil.calValidity(ticker, tradingTime);
				int preHoldings = resultSet.getInt("PreHoldings");
				type = futuresPropertyUtil.calType(preHoldings);
				superFutures = futuresPropertyUtil.calSuperFutures();

				// parameters in FuturesPosition table
				nature = futuresPositionUtil.calNature();

				// parameters in kchartsday table
				closePrice = currentPrice;
				amount = resultSet.getBigDecimal("TurnOver");

				// set the value in futuresDetails
				System.out.println("    >>> " + resultSet.getRow()
						+ " FuturesDetailsRecord >>> " + ticker + ","
						+ datetime + "," + currentPrice + "," + yesterdaySettle
						+ "," + hightAndLow + "," + hightAndLowRange + ","
						+ fiveUpdown + "," + yearToDate + "," + openPrice + ","
						+ volumnCount + "," + nowHand + "," + highPrice + ","
						+ lowPrice + "," + position + "," + loadingUp + ","
						+ volume + "," + turnover + ", " + estimationSettle
						+ "," + outerDisk + "," + disk + "," + tradingSentiment
						+ "," + windVane + "," + massOfPublicOpinion);

				detailsPrestmt.setString(FuturesDetails.Ticker.ordinal() + 1,
						ticker);
				detailsPrestmt
						.setBigDecimal(
								FuturesDetails.CurrentPrice.ordinal() + 1,
								currentPrice);
				detailsPrestmt.setFloat(
						FuturesDetails.HightAndLow.ordinal() + 1, hightAndLow);
				detailsPrestmt.setFloat(
						FuturesDetails.HightAndLowRange.ordinal() + 1,
						hightAndLowRange);
				detailsPrestmt.setFloat(
						FuturesDetails.Fiveupdown.ordinal() + 1, fiveUpdown);
				detailsPrestmt.setFloat(
						FuturesDetails.YearToDate.ordinal() + 1, yearToDate);
				detailsPrestmt.setTimestamp(
						FuturesDetails.Datetime.ordinal() + 1, datetime);
				detailsPrestmt.setBigDecimal(
						FuturesDetails.YesterdaySettle.ordinal() + 1,
						yesterdaySettle);
				detailsPrestmt.setBigDecimal(
						FuturesDetails.OpenPrice.ordinal() + 1, openPrice);
				detailsPrestmt.setLong(FuturesDetails.NowHand.ordinal() + 1,
						nowHand.longValue());
				detailsPrestmt.setLong(
						FuturesDetails.VolumnCount.ordinal() + 1,
						volumnCount.longValue());
				detailsPrestmt.setBigDecimal(
						FuturesDetails.HighPrice.ordinal() + 1, highPrice);
				detailsPrestmt.setBigDecimal(
						FuturesDetails.LowPrice.ordinal() + 1, lowPrice);
				detailsPrestmt.setLong(FuturesDetails.LoadingUp.ordinal() + 1,
						loadingUp.longValue());
				detailsPrestmt.setLong(FuturesDetails.Position.ordinal() + 1,
						position.longValue());
				detailsPrestmt.setLong(FuturesDetails.Volume.ordinal() + 1,
						volume.longValue());
				detailsPrestmt.setBigDecimal(
						FuturesDetails.Turnover.ordinal() + 1, turnover);
				detailsPrestmt.setBigDecimal(
						FuturesDetails.EstimationSettle.ordinal() + 1,
						estimationSettle);
				detailsPrestmt.setLong(FuturesDetails.OuterDisk.ordinal() + 1,
						outerDisk.longValue());
				detailsPrestmt.setLong(FuturesDetails.Disk.ordinal() + 1,
						disk.longValue());
				detailsPrestmt.setFloat(
						FuturesDetails.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				detailsPrestmt.setFloat(FuturesDetails.WindVane.ordinal() + 1,
						windVane);
				detailsPrestmt.setFloat(
						FuturesDetails.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				// set the values in futuresProperty table
				System.out.println("    >>> " + resultSet.getRow()
						+ " FuturesPropertyRecord >>> " + ticker + ","
						+ tickerName + "," + listingDate + "," + updateDate
						+ "," + validity + "," + type + "," + superFutures);

				propertyPrestmt.setString(FuturesProperty.Ticker.ordinal() + 1,
						ticker);
				propertyPrestmt.setString(
						FuturesProperty.TickerName.ordinal() + 1, tickerName);
				propertyPrestmt.setDate(
						FuturesProperty.ListingDate.ordinal() + 1, listingDate);
				propertyPrestmt.setDate(
						FuturesProperty.UpdateDate.ordinal() + 1, updateDate);
				propertyPrestmt.setLong(FuturesProperty.Validity.ordinal() + 1,
						validity);
				propertyPrestmt.setLong(FuturesProperty.Type.ordinal() + 1,
						type);
				propertyPrestmt.setString(
						FuturesProperty.SuperFutures.ordinal() + 1,
						superFutures);

				// set the values in futuresposition table
				System.out.println("    >>> " + resultSet.getRow()
						+ " FuturesPositionRecord");
				positionPrestmt.setString(FuturesPosition.Ticker.ordinal() + 1,
						ticker);
				positionPrestmt.setTimestamp(
						FuturesPosition.Datetime.ordinal() + 1, datetime);
				positionPrestmt.setBigDecimal(
						FuturesPosition.Price.ordinal() + 1, currentPrice);
				positionPrestmt.setLong(FuturesPosition.NowHand.ordinal() + 1,
						nowHand.longValue());
				positionPrestmt.setLong(
						FuturesPosition.LoadingUp.ordinal() + 1,
						loadingUp.longValue());
				positionPrestmt.setFloat(FuturesPosition.Nature.ordinal() + 1,
						nature);

				// set the values in kchartsday table
				System.out.println("    >>> " + resultSet.getRow()
						+ " KChartsDayRecord");
				kChartsDayPrestmt.setString(KChartsDay.Ticker.ordinal() + 1,
						ticker);
				kChartsDayPrestmt.setDate(
						KChartsDay.Datetime.ordinal() + 1, new Date(datetime.getTime()));
				kChartsDayPrestmt.setBigDecimal(
						KChartsDay.OpenPrice.ordinal() + 1, openPrice);
				kChartsDayPrestmt.setBigDecimal(
						KChartsDay.HightPrice.ordinal() + 1, highPrice);
				kChartsDayPrestmt.setBigDecimal(
						KChartsDay.LowPrice.ordinal() + 1, lowPrice);
				kChartsDayPrestmt.setBigDecimal(
						KChartsDay.ClosePrice.ordinal() + 1, closePrice);
				kChartsDayPrestmt.setLong(KChartsDay.Volume.ordinal() + 1,
						volume.longValue());
				kChartsDayPrestmt.setBigDecimal(
						KChartsDay.Amount.ordinal() + 1, amount);
				kChartsDayPrestmt.setFloat(
						KChartsDay.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				kChartsDayPrestmt.setFloat(KChartsDay.WindVane.ordinal() + 1,
						windVane);
				kChartsDayPrestmt.setFloat(
						KChartsDay.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				detailsPrestmt.executeUpdate();
				propertyPrestmt.executeUpdate();
				positionPrestmt.executeUpdate();
				kChartsDayPrestmt.executeUpdate();
			}

			// detailsPrestmt.close();
			// propertyPrestmt.close();
			// conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void closeStatement() {
		try {
			detailsPrestmt.close();
			propertyPrestmt.close();
			kChartsDayPrestmt.close();
			positionPrestmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
