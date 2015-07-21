package com.rongdata.dbUtil;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

public class FuturesUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;

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
		String sourceSql = "select * from (select TradingTime, ContractId, PreSettlementPrice, "
				+ "CurrSettlementPrice, CurrOpenPrice, PreHoldings, Holdings, LatestPrice, Volume, "
				+ "Turnover, TopPrice, BottomPrice, BidPrice1, AskPrice1, BidVolume1, AskVolume1, "
				+ "BidPrice2, AskPrice2, BidVolume2, AskVolume2, BidPrice3, AskPrice3, "
				+ "BidVolume3, AskVolume3 from xcube.futures_quotation as a "
				+ "where TradingTime=(select TradingTime from xcube.latest_futures_tradingtime "
				+ "where a.ContractId=contractid)) as b group by contractid";

		String detailsSql = "insert ignore into xcube.com_futures_details(Ticker, CurrentPrice, "
				+ "HightAndLow, HightAndLowRange, 5updown, YearToDate, Datetime, "
				+ "YesterdaySettle, OpenPrice, NowHand, VolumnCount, HighPrice, LowPrice, "
				+ "LoadingUp, Position, Volume, Turnover, EstimationSettle, OuterDisk, Disk, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) values(?, ?, ?, "
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		String propertySql = "insert ignore into xcube.com_futures_property(Ticker, TickerName, "
				+ "ListingDate, UpdateDate, Validity, Type, SuperFutures) values(?, ?, ?, "
				+ "?, ?, ?, ?)";
		
		// update futures data into kchartsday table
 		String kChartsDaySql = "replace into xcube.com_k_charts_day(Ticker, Datetime, "
				+ "OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

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
		float turnover = 0; // 成交金额
		float estimationSettle = 0; // 估结算
		float outerDisk = 0; // 外盘
		float disk = 0; // 内盘
		float tradingSentiment = 0; // 交易情绪
		float windVane = 0; // 大众舆情
		float massOfPublicOpinion = 0; // 大众舆情

		String tickerName = ticker;
		Date listingDate = null;
		Date updateDate = null;
		int validity = 1;
		int type = 0;
		String superFutures = null;
		
		// parameters in kchartsday table
		float closePrice = 0;	// 收盘价格
		float amount = 0;	// 累计成交金额

		if (conn == null) {
			System.out.println("futuresutil.operate >>> reconstruct conn");
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}

		try {
			PreparedStatement detailsPrestmt = conn
					.prepareStatement(detailsSql);
			PreparedStatement propertyPrestmt = conn
					.prepareStatement(propertySql);
			PreparedStatement kChartsDayPrestmt = conn.prepareStatement(kChartsDaySql);
			
			FuturesDetailsUtil futuresDetailsUtil = new FuturesDetailsUtil(conn);
			FuturesPropertyUtil futuresPropertyUtil = new FuturesPropertyUtil(
					conn);
			

			while (resultSet.next()) {
				// parameters in FuturesDetails table
				ticker = resultSet.getString("ContractId");
				datetime = resultSet.getTimestamp("TradingTime");
				currentPrice = resultSet.getFloat("LatestPrice");
				yesterdaySettle = resultSet.getFloat("PreSettlementPrice");
				hightAndLow = futuresDetailsUtil.calHightAndLow(currentPrice,
						yesterdaySettle);
				hightAndLowRange = futuresDetailsUtil.calHightAndLowRange(
						currentPrice, yesterdaySettle);
				// create a table to store the preSettlementPrice
				fiveUpdown = futuresDetailsUtil.calFiveUpdown(ticker,
						currentPrice, datetime);
				yearToDate = futuresDetailsUtil.calYearToDate(ticker,
						currentPrice, datetime);
				openPrice = resultSet.getFloat("CurrOpenPrice");
				// volume in market_quotation means cumvolume
				volumnCount = resultSet.getFloat("Volume");
				nowHand = futuresDetailsUtil.calNowHand(ticker, datetime,
						volumnCount);
				highPrice = resultSet.getFloat("TopPrice");
				lowPrice = resultSet.getFloat("BottomPrice");
				position = resultSet.getFloat("Holdings");
				loadingUp = futuresDetailsUtil.calLoadingUp(ticker, datetime,
						position);
				volume = nowHand;
				estimationSettle = resultSet.getFloat("CurrSettlementPrice");
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
				
				// parameters in kchartsday table
				closePrice = currentPrice;
				amount = resultSet.getFloat("TurnOver");

				// set the value in futuresDetails
				System.out.println(resultSet.getRow()
						+ " FuturesDetailsRecord >>> " + ticker + ","
						+ datetime + "," + currentPrice + "," + yesterdaySettle
						+ "," + hightAndLow + "," + hightAndLowRange + ","
						+ fiveUpdown + "," + yearToDate + "," + openPrice + ","
						+ volumnCount + "," + nowHand + "," + highPrice + ","
						+ lowPrice + "," + position + "," + loadingUp + ","
						+ volume + "," + turnover + ", " + estimationSettle + "," + outerDisk
						+ "," + disk + "," + tradingSentiment + "," + windVane
						+ "," + massOfPublicOpinion);

				detailsPrestmt.setString(FuturesDetails.Ticker.ordinal() + 1,
						ticker);
				detailsPrestmt
						.setFloat(FuturesDetails.CurrentPrice.ordinal() + 1,
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
				detailsPrestmt.setFloat(
						FuturesDetails.YesterdaySettle.ordinal() + 1,
						yesterdaySettle);
				detailsPrestmt.setFloat(FuturesDetails.OpenPrice.ordinal() + 1,
						openPrice);
				detailsPrestmt.setFloat(FuturesDetails.NowHand.ordinal() + 1,
						nowHand);
				detailsPrestmt.setFloat(
						FuturesDetails.VolumnCount.ordinal() + 1, volumnCount);
				detailsPrestmt.setFloat(FuturesDetails.HighPrice.ordinal() + 1,
						highPrice);
				detailsPrestmt.setFloat(FuturesDetails.LowPrice.ordinal() + 1,
						lowPrice);
				detailsPrestmt.setFloat(FuturesDetails.LoadingUp.ordinal() + 1,
						loadingUp);
				detailsPrestmt.setFloat(FuturesDetails.Position.ordinal() + 1,
						position);
				detailsPrestmt.setFloat(FuturesDetails.Volume.ordinal() + 1,
						volume);
				detailsPrestmt.setFloat(FuturesDetails.Turnover.ordinal() + 1, turnover);
				detailsPrestmt.setFloat(
						FuturesDetails.EstimationSettle.ordinal() + 1,
						estimationSettle);
				detailsPrestmt.setFloat(FuturesDetails.OuterDisk.ordinal() + 1,
						outerDisk);
				detailsPrestmt
						.setFloat(FuturesDetails.Disk.ordinal() + 1, disk);
				detailsPrestmt.setFloat(
						FuturesDetails.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				detailsPrestmt.setFloat(FuturesDetails.WindVane.ordinal() + 1,
						windVane);
				detailsPrestmt.setFloat(
						FuturesDetails.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				// set the values in futuresProperty table
				System.out.println(resultSet.getRow()
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
				propertyPrestmt.setInt(FuturesProperty.Validity.ordinal() + 1,
						validity);
				propertyPrestmt
						.setInt(FuturesProperty.Type.ordinal() + 1, type);
				propertyPrestmt.setString(
						FuturesProperty.SuperFutures.ordinal() + 1,
						superFutures);
				
				// set the values in kchartsday table
				kChartsDayPrestmt.setString(KChartsDay.Ticker.ordinal() + 1, ticker);
				kChartsDayPrestmt.setTimestamp(KChartsDay.Datetime.ordinal() + 1, datetime);
				kChartsDayPrestmt.setFloat(KChartsDay.OpenPrice.ordinal() + 1, openPrice);
				kChartsDayPrestmt.setFloat(KChartsDay.HightPrice.ordinal() + 1, highPrice);
				kChartsDayPrestmt.setFloat(KChartsDay.LowPrice.ordinal() + 1, lowPrice);
				kChartsDayPrestmt.setFloat(KChartsDay.ClosePrice.ordinal() + 1, closePrice);
				kChartsDayPrestmt.setFloat(KChartsDay.Volume.ordinal() + 1, volume);
				kChartsDayPrestmt.setFloat(KChartsDay.Amount.ordinal() + 1, amount);
				kChartsDayPrestmt.setFloat(KChartsDay.TradingSentiment.ordinal() + 1, tradingSentiment);
				kChartsDayPrestmt.setFloat(KChartsDay.WindVane.ordinal() + 1, windVane);
				kChartsDayPrestmt.setFloat(KChartsDay.MassOfPublicOpinion.ordinal() + 1, massOfPublicOpinion);

				detailsPrestmt.execute();
				propertyPrestmt.execute();
				kChartsDayPrestmt.execute();
			}

//			detailsPrestmt.close();
//			propertyPrestmt.close();
//			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
