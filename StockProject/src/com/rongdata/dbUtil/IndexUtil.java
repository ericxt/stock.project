package com.rongdata.dbUtil;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;

import com.mysql.jdbc.Connection;

public class IndexUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;
	private boolean firstMainTickerOperation = true;
	private HashSet<String> mainTickerSet = new HashSet<String>();

	public IndexUtil() {
		// TODO Auto-generated constructor stub
	}

	public IndexUtil(Connection conn) {
		this.conn = conn;
	}

	public IndexUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public IndexUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	public void operate() {
		String sourceSql = "select * from (select contractid, latestprice, presettlementprice, "
				+ "holdings, preholdings, tradingtime, CurrOpenPrice, Volume, Turnover, TopPrice, "
				+ "BottomPrice, CurrSettlementPrice from xcube.index_quotation as a "
				+ "where TradingTime=(select TradingTime from xcube.latest_index_tradingtime "
				+ "where a.ContractId=contractid)) as b group by contractid";

		String detailsSql = "insert ignore into xcube.com_index_details(StockCode, CurrentPrice, "
				+ "HightAndLow, HightAndLowRange, Basis, DayLoadingUp, Datetime,YesterdaySettle, "
				+ "OpenPrice, NowHand, VolumnCount, Turnover, HighPrice, LowPrice, Position, LoadingUp, "
				+ "EstimationSettle, TradingSentiment, WindVane, MassOfPublicOpinion) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		String propertySql = "insert ignore into xcube.com_index_property(Ticker, TickerName, "
				+ "ListingDate, UpdateDate, Validity, Type, PTicker) values(?, ?, ?, "
				+ "?, ?, ?, ?)";

		// update index data into kchartsday table
		String kChartsDaySql = "replace into xcube.com_k_charts_day(Ticker, Datetime, "
				+ "OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		String stockCode = null; // 期指代码
		float currentPrice = 0; // 当前价格
		float hightAndLow = 0; // 涨跌
		float hightAndLowRange = 0; // 涨跌幅
		float basis = 0; // 基差
		float dayLoadingUp = 0; // 日增仓
		Timestamp datetime = null; // 时间
		float yesterdaySettle = 0; // 昨结
		float openPrice = 0; // 开盘
		float nowHand = 0; // 现手
		float volumnCount = 0; // 总手
		float turnover = 0; // 成交金额
		float highPrice = 0; // 最高价
		float lowPrice = 0; // 最低价
		float position = 0; // 持仓
		float loadingUp = 0; // 增仓
		float estimationSettle = 0; // 估结算
		float tradingSentiment = 0; // 交易情绪
		float windVane = 0; // 风向标
		float massOfPublicOpinion = 0; // 大众舆情

		String tickerName = null; // 期指名称
		Date listingDate = null; // 上市日期
		Date updateDate = null; // 更新日期
		int validity = 1; // 1有效 0无效
		int type = 1; // 1主力合约 2 非主力合约
		String pTicker = null; // 父期指

		// parameters in kchartsday table
		float closePrice = 0; // 收盘价格
		float amount = 0; // 累计成交金额

		if (conn == null) {
			System.out.println("indexutil.operate >>> reconstruct conn");
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
			PreparedStatement kChartsDayPrestmt = conn
					.prepareStatement(kChartsDaySql);

			IndexDetailsUtil indexDetailsUtil = new IndexDetailsUtil(conn);
			IndextPropertyUtil indextPropertyUtil = new IndextPropertyUtil(conn);

			if (firstMainTickerOperation) {
				mainTickerSet = indextPropertyUtil.getMainTicker();
				firstMainTickerOperation = false;
			}

			while (resultSet.next()) {
				// parameters in IndexDetails talbe
				stockCode = resultSet.getString("ContractId");
				currentPrice = resultSet.getFloat("LatestPrice");
				yesterdaySettle = resultSet.getFloat("PreSettlementPrice");
				hightAndLow = indexDetailsUtil.calHightAndLow(currentPrice,
						yesterdaySettle);
				hightAndLowRange = indexDetailsUtil.calHightAndLowRange(
						currentPrice, yesterdaySettle);
				basis = indexDetailsUtil.calBasis(stockCode, currentPrice);
				position = resultSet.getFloat("Holdings");
				float preHoldings = resultSet.getFloat("PreHoldings");
				dayLoadingUp = indexDetailsUtil.calDayLoadingUp(position,
						preHoldings);
				datetime = resultSet.getTimestamp("TradingTime");
				openPrice = resultSet.getFloat("CurrOpenPrice");
				volumnCount = resultSet.getFloat("Volume");
				nowHand = indexDetailsUtil.calNowHand(stockCode, datetime,
						volumnCount);
				turnover = resultSet.getFloat("Turnover");
				highPrice = resultSet.getFloat("TopPrice");
				lowPrice = resultSet.getFloat("BottomPrice");
				loadingUp = indexDetailsUtil.calLoadingUp(stockCode, datetime,
						position);
				estimationSettle = resultSet.getFloat("CurrSettlementPrice");
				tradingSentiment = indexDetailsUtil.calTradingSentiment(
						stockCode, datetime);
				windVane = indexDetailsUtil.calWindVane();
				massOfPublicOpinion = indexDetailsUtil.calMassOfPublicOpinion();

				// parameters in FuturesProperty talbe
				tickerName = stockCode;
				listingDate = indextPropertyUtil.calListingDate(stockCode);
				updateDate = indextPropertyUtil.calUpdateDate();
				Timestamp tradingTime = resultSet.getTimestamp("TradingTime");
				validity = indextPropertyUtil.calValidity(stockCode,
						tradingTime);
				// int preHoldings = resultSet.getInt("PreHoldings");
				type = (mainTickerSet.contains(stockCode)) ? 1 : 0;
				pTicker = indextPropertyUtil.calPTicker();

				// parameters in kchartsday table
				closePrice = currentPrice;
				amount = turnover;

				// set the values in IndexDetails table
				System.out.println(resultSet.getRow()
						+ " IndexDetailsRecord >>> " + stockCode + ","
						+ datetime + "," + currentPrice + "," + yesterdaySettle
						+ "," + hightAndLow + "," + hightAndLowRange + ","
						+ basis + "," + position + "," + openPrice + ","
						+ volumnCount + "," + turnover + ", " + nowHand + ","
						+ highPrice + "," + lowPrice + "," + position + ","
						+ loadingUp + "," + estimationSettle + ","
						+ tradingSentiment + "," + windVane + ","
						+ massOfPublicOpinion);
				detailsPrestmt.setString(IndexDetails.StockCode.ordinal() + 1,
						stockCode);
				detailsPrestmt.setFloat(
						IndexDetails.CurrentPrice.ordinal() + 1, currentPrice);
				detailsPrestmt.setFloat(IndexDetails.HightAndLow.ordinal() + 1,
						hightAndLow);
				detailsPrestmt.setFloat(
						IndexDetails.HightAndLowRange.ordinal() + 1,
						hightAndLowRange);
				detailsPrestmt
						.setFloat(IndexDetails.Basis.ordinal() + 1, basis);
				detailsPrestmt.setFloat(
						IndexDetails.DayLoadingUp.ordinal() + 1, dayLoadingUp);
				detailsPrestmt.setTimestamp(
						IndexDetails.Datetime.ordinal() + 1, datetime);
				detailsPrestmt.setFloat(
						IndexDetails.YesterdaySettle.ordinal() + 1,
						yesterdaySettle);
				detailsPrestmt.setFloat(IndexDetails.OpenPrice.ordinal() + 1,
						openPrice);
				detailsPrestmt.setFloat(IndexDetails.NowHand.ordinal() + 1,
						nowHand);
				detailsPrestmt.setFloat(IndexDetails.VolumnCount.ordinal() + 1,
						volumnCount);
				detailsPrestmt.setFloat(IndexDetails.Turnover.ordinal() + 1,
						turnover);
				detailsPrestmt.setFloat(IndexDetails.HighPrice.ordinal() + 1,
						highPrice);
				detailsPrestmt.setFloat(IndexDetails.LowPrice.ordinal() + 1,
						lowPrice);
				detailsPrestmt.setFloat(IndexDetails.Position.ordinal() + 1,
						position);
				detailsPrestmt.setFloat(IndexDetails.LoadingUp.ordinal() + 1,
						loadingUp);
				detailsPrestmt.setFloat(
						IndexDetails.EstimationSettle.ordinal() + 1,
						estimationSettle);
				detailsPrestmt.setFloat(
						IndexDetails.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				detailsPrestmt.setFloat(IndexDetails.WindVane.ordinal() + 1,
						windVane);
				detailsPrestmt.setFloat(
						IndexDetails.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				// set the values in IndexProperty table
				System.out.println(resultSet.getRow()
						+ " IndexPropertyRecord >>> " + stockCode + ","
						+ tickerName + "," + listingDate + "," + updateDate
						+ "," + validity + "," + type + "," + pTicker);
				propertyPrestmt.setString(IndexProperty.Ticker.ordinal() + 1,
						stockCode);
				propertyPrestmt.setString(
						IndexProperty.TickerName.ordinal() + 1, tickerName);
				propertyPrestmt.setDate(
						IndexProperty.ListingDate.ordinal() + 1, listingDate);
				propertyPrestmt.setDate(IndexProperty.UpdateDate.ordinal() + 1,
						updateDate);
				propertyPrestmt.setInt(IndexProperty.Validity.ordinal() + 1,
						validity);
				propertyPrestmt.setInt(IndexProperty.Type.ordinal() + 1, type);
				propertyPrestmt.setString(IndexProperty.PTicker.ordinal() + 1,
						pTicker);

				// set the values in kchartsday table
				kChartsDayPrestmt.setString(KChartsDay.Ticker.ordinal() + 1,
						stockCode);
				kChartsDayPrestmt.setTimestamp(
						KChartsDay.Datetime.ordinal() + 1, datetime);
				kChartsDayPrestmt.setFloat(KChartsDay.OpenPrice.ordinal() + 1,
						openPrice);
				kChartsDayPrestmt.setFloat(KChartsDay.HightPrice.ordinal() + 1,
						highPrice);
				kChartsDayPrestmt.setFloat(KChartsDay.LowPrice.ordinal() + 1,
						lowPrice);
				kChartsDayPrestmt.setFloat(KChartsDay.ClosePrice.ordinal() + 1,
						closePrice);
				kChartsDayPrestmt.setFloat(KChartsDay.Volume.ordinal() + 1,
						volumnCount);
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
				kChartsDayPrestmt.execute();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
