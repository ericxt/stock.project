package com.rongdata.dbUtil;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

public class StockUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;

	public StockUtil() {
		// TODO Auto-generated constructor stub
	}

	public StockUtil(Connection conn) {
		this.conn = conn;
	}

	public StockUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public StockUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	public void operate() {
		String sourceSql = "select * from (select TradingTime, ContractId, PreClosePrice, "
				+ "CurrOpenPrice, Holdings, LatestPrice, Volume, Turnover, AveragePrice, "
				+ "TopPrice, BottomPrice from xcube.stock_quotation as a "
				+ "where TradingTime=(select TradingTime from xcube.latest_stock_tradingtime "
				+ "where a.ContractId=contractid)) as b group by contractid";

		String detailsSql = "insert ignore into xcube.com_single_ticker_details(StockCode, "
				+ "Datetime, CurrentPrice, HightAndLow, HightAndLowRange, YesterdayReceived, "
				+ "TodayOpenPrice, Volume, Turnover, TurnoverRate, HightPrice, LowPrice, "
				+ "AveragePrice, PriceEarningsRatio, VolumeCount, VolumeRatio, PriceToBook, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion, TotalMarketValue, "
				+ "CirculationValue) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ "?, ?, ?, ?, ?);";

		String PropertySql = "insert ignore into xcube.com_single_ticker_property(TickerName, "
				+ "TickerCode, ListingDate, UpdateDate, Validity) values(?, ?, ?, ?, ?)";

		String positionSql = "replace into xcube.com_ticker_position(Ticker, Datetime, "
				+ "Price, NowHand) value(?, ?, ?, ?)";

		// update stock data into kchartsday table
		String kChartsDaySql = "replace into xcube.com_k_charts_day(Ticker, Datetime, "
				+ "OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

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

		String tickerName = null; // 股票名称
		String tickerCode = null; // 股票代码
		Date listingDate = null; // 上市日期
		Date updateDate = null; // 更新日期
		int validity = 1; // 1有效 0无效

		float nowHand = 0;

		// parameters in kchartsday table
		float closePrice = 0; // 收盘价格
		float amount = 0; // 累计成交金额

		if (conn == null) {
			System.out.println("StockUtil.operate >>> reconstruct conn");
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}

		try {
			PreparedStatement detailsPrestmt = conn
					.prepareStatement(detailsSql);
			PreparedStatement propertyPrestmt = conn
					.prepareStatement(PropertySql);
			PreparedStatement positionPrestmt = conn
					.prepareStatement(positionSql);
			PreparedStatement kChartsDayPrestmt = conn.prepareStatement(kChartsDaySql);

			SingleTickerDetailsUtil singleTickerDetailsUtil = new SingleTickerDetailsUtil(
					conn);
			SingleTickerPropertyUtil singleTickerPropertyUtil = new SingleTickerPropertyUtil(
					conn);
			TickerPositionUtil tickerPositionUtil = new TickerPositionUtil(conn);

			while (resultSet.next()) {
				// parameters in SingleTickerDetails table
				stockCode = resultSet.getString("ContractId");
				datetime = resultSet.getTimestamp("TradingTime");
				currentPrice = resultSet.getFloat("LatestPrice");
				yesterdayReceived = resultSet.getFloat("PreClosePrice");
				hightAndLow = singleTickerDetailsUtil.calHightAndLow(
						currentPrice, yesterdayReceived);
				hightAndLowRange = singleTickerDetailsUtil.calHightAndLowRange(
						currentPrice, yesterdayReceived);
				todayOpenPrice = resultSet.getFloat("CurrOpenPrice");
				volume = resultSet.getInt("Volume");
				turnover = resultSet.getFloat("TurnOver");
				turnoverRate = singleTickerDetailsUtil.calTurnoverRate(volume);
				hightPrice = resultSet.getFloat("TopPrice");
				lowPrice = resultSet.getFloat("BottomPrice");
				averagePrice = resultSet.getFloat("AveragePrice");
				priceEarningsRatio = singleTickerDetailsUtil
						.calPriceEarningsRatio(currentPrice);
				volumeCount = volume;
				volumeRatio = singleTickerDetailsUtil.calVolumeRatio(stockCode,
						datetime, volume);
				priceToBook = singleTickerDetailsUtil
						.calPriceToBook(currentPrice);
				tradingSentiment = singleTickerDetailsUtil.calTradingSentiment(
						stockCode, datetime);
				windVane = singleTickerDetailsUtil.calWindVane();
				massOfPublicOpinion = singleTickerDetailsUtil
						.calMassOfPublicOpinion();
				totalMarketValue = singleTickerDetailsUtil
						.calTotalMarketValue(currentPrice);
				circulationValue = singleTickerDetailsUtil
						.calCirculationValue(currentPrice);

				// parameters in SingleTickerProperty table
				tickerCode = stockCode;
				tickerName = tickerCode;
				listingDate = singleTickerPropertyUtil
						.calListingDate(tickerCode);
				updateDate = singleTickerPropertyUtil.calUpdateDate();
				Timestamp tradingTime = resultSet.getTimestamp("TradingTime");
				validity = singleTickerPropertyUtil.calValidity(tickerCode,
						tradingTime);

				// parameters in TickerPosition table
				nowHand = tickerPositionUtil.calNowHand(stockCode, datetime,
						volume);
				
				// parameters in kchartsday table
				closePrice = currentPrice;
				amount = turnover;

				// set the value in stockDetails
				System.out.println(resultSet.getRow()
						+ " StockDetailsRecord >>> " + stockCode + ","
						+ datetime + "," + currentPrice + ","
						+ yesterdayReceived + "," + hightAndLow + ","
						+ hightAndLowRange + "," + todayOpenPrice + ","
						+ volume + "," + turnover + "," + turnoverRate + ","
						+ hightPrice + ", " + lowPrice + "," + averagePrice
						+ "," + priceEarningsRatio + "," + volumeCount + ","
						+ volumeRatio + "," + priceToBook + ","
						+ tradingSentiment + "," + windVane + ","
						+ massOfPublicOpinion);
				detailsPrestmt.setString(
						SingleTickerDetails.StockCode.ordinal() + 1, stockCode);
				detailsPrestmt.setTimestamp(
						SingleTickerDetails.Datetime.ordinal() + 1, datetime);
				detailsPrestmt.setFloat(
						SingleTickerDetails.CurrentPrice.ordinal() + 1,
						currentPrice);
				detailsPrestmt.setFloat(
						SingleTickerDetails.HightAndLow.ordinal() + 1,
						hightAndLow);
				detailsPrestmt.setFloat(
						SingleTickerDetails.HightAndLowRange.ordinal() + 1,
						hightAndLowRange);
				detailsPrestmt.setFloat(
						SingleTickerDetails.YesterdayReceived.ordinal() + 1,
						yesterdayReceived);
				detailsPrestmt.setFloat(
						SingleTickerDetails.TodayOpenPrice.ordinal() + 1,
						todayOpenPrice);
				detailsPrestmt.setFloat(
						SingleTickerDetails.Volume.ordinal() + 1, volume);
				detailsPrestmt.setFloat(
						SingleTickerDetails.Turnover.ordinal() + 1, turnover);
				detailsPrestmt.setFloat(
						SingleTickerDetails.TurnoverRate.ordinal() + 1,
						turnoverRate);
				detailsPrestmt.setFloat(
						SingleTickerDetails.HightPrice.ordinal() + 1,
						hightPrice);
				detailsPrestmt.setFloat(
						SingleTickerDetails.LowPrice.ordinal() + 1, lowPrice);
				detailsPrestmt.setFloat(
						SingleTickerDetails.AveragePrice.ordinal() + 1,
						averagePrice);
				detailsPrestmt.setFloat(
						SingleTickerDetails.PriceEarningsRatio.ordinal() + 1,
						priceEarningsRatio);
				detailsPrestmt.setFloat(
						SingleTickerDetails.VolumeCount.ordinal() + 1,
						volumeCount);
				detailsPrestmt.setFloat(
						SingleTickerDetails.VolumeRatio.ordinal() + 1,
						volumeRatio);
				detailsPrestmt.setFloat(
						SingleTickerDetails.PriceToBook.ordinal() + 1,
						priceToBook);
				detailsPrestmt.setFloat(
						SingleTickerDetails.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				detailsPrestmt.setFloat(
						SingleTickerDetails.WindVane.ordinal() + 1, windVane);
				detailsPrestmt.setFloat(
						SingleTickerDetails.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);
				detailsPrestmt.setFloat(
						SingleTickerDetails.TotalMarketValue.ordinal() + 1,
						totalMarketValue);
				detailsPrestmt.setFloat(
						SingleTickerDetails.CirculationValue.ordinal() + 1,
						circulationValue);

				// set the values in StockProperty table
				System.out.println(resultSet.getRow()
						+ " StockPropertyRecord >>> " + tickerName + ","
						+ tickerCode + "," + listingDate + "," + updateDate
						+ "," + validity);
				propertyPrestmt.setString(
						SingleTickerProperty.TickerName.ordinal() + 1,
						tickerName);
				propertyPrestmt.setString(
						SingleTickerProperty.TickerCode.ordinal() + 1,
						tickerCode);
				propertyPrestmt.setDate(
						SingleTickerProperty.ListingDate.ordinal() + 1,
						listingDate);
				propertyPrestmt.setDate(
						SingleTickerProperty.UpdateDate.ordinal() + 1,
						updateDate);
				propertyPrestmt.setInt(
						SingleTickerProperty.Validity.ordinal() + 1, validity);

				// set the values in TickerPosition table
				positionPrestmt.setString(TickerPosition.Ticker.ordinal() + 1,
						stockCode);
				positionPrestmt.setTimestamp(
						TickerPosition.Datetime.ordinal() + 1, datetime);
				positionPrestmt.setFloat(TickerPosition.Price.ordinal() + 1,
						currentPrice);
				positionPrestmt.setFloat(TickerPosition.NowHand.ordinal() + 1,
						nowHand);
				
				// set the values in kchartsday table
				kChartsDayPrestmt.setString(KChartsDay.Ticker.ordinal() + 1,
						stockCode);
				kChartsDayPrestmt.setTimestamp(
						KChartsDay.Datetime.ordinal() + 1, datetime);
				kChartsDayPrestmt.setFloat(KChartsDay.OpenPrice.ordinal() + 1,
						todayOpenPrice);
				kChartsDayPrestmt.setFloat(KChartsDay.HightPrice.ordinal() + 1,
						hightPrice);
				kChartsDayPrestmt.setFloat(KChartsDay.LowPrice.ordinal() + 1,
						lowPrice);
				kChartsDayPrestmt.setFloat(KChartsDay.ClosePrice.ordinal() + 1,
						closePrice);
				kChartsDayPrestmt.setFloat(KChartsDay.Volume.ordinal() + 1,
						volume);
				kChartsDayPrestmt.setFloat(KChartsDay.Amount.ordinal() + 1,
						amount);
				kChartsDayPrestmt.setFloat(
						KChartsDay.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				kChartsDayPrestmt.setFloat(KChartsDay.WindVane.ordinal() + 1,
						windVane);
				kChartsDayPrestmt.setFloat(
						KChartsDay.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				detailsPrestmt.execute();
				propertyPrestmt.execute();
				positionPrestmt.execute();
				kChartsDayPrestmt.execute();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
