package com.rongdata.dbUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
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

		String detailsSql = "replace into xcube.com_index_details(StockCode, CurrentPrice, "
				+ "HightAndLow, HightAndLowRange, Basis, DayLoadingUp, Datetime,YesterdaySettle, "
				+ "OpenPrice, NowHand, VolumnCount, Turnover, HighPrice, LowPrice, Position, LoadingUp, "
				+ "EstimationSettle, TradingSentiment, WindVane, MassOfPublicOpinion) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		String propertySql = "replace into xcube.com_index_property(Ticker, TickerName, "
				+ "ListingDate, UpdateDate, Validity, Type, PTicker) values(?, ?, ?, "
				+ "?, ?, ?, ?)";

		String positionSql = "replace into xcube.com_futures_position(Ticker, Datetime, "
				+ "Price, NowHand, LoadingUp, Nature) values(?, ?, ?, ?, ?, ?)";

		// update index data into kchartsday table
		String kChartsDaySql = "replace into xcube.com_k_charts_day(Ticker, Datetime, "
				+ "OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

		// parameters in indexdetails table
		String stockCode = null; // 期指代码
		BigDecimal currentPrice = BigDecimal.ZERO; // 当前价格
		float hightAndLow = 0; // 涨跌
		float hightAndLowRange = 0; // 涨跌幅
		float basis = 0; // 基差
		BigInteger dayLoadingUp = BigInteger.ZERO; // 日增仓
		Timestamp datetime = null; // 时间
		BigDecimal yesterdaySettle = BigDecimal.ZERO; // 昨结
		BigDecimal openPrice = BigDecimal.ZERO; // 开盘
		BigInteger nowHand = BigInteger.ZERO; // 现手
		BigInteger volumnCount = BigInteger.ZERO; // 总手
		BigDecimal turnover = BigDecimal.ZERO; // 成交金额
		BigDecimal highPrice = BigDecimal.ZERO; // 最高价
		BigDecimal lowPrice = BigDecimal.ZERO; // 最低价
		BigInteger position = BigInteger.ZERO; // 持仓
		BigInteger loadingUp = BigInteger.ZERO; // 增仓
		BigDecimal estimationSettle = BigDecimal.ZERO; // 估结算
		float tradingSentiment = 0; // 交易情绪
		float windVane = 0; // 风向标
		float massOfPublicOpinion = 0; // 大众舆情

		// parameters in indexproperty table
		String tickerName = null; // 期指名称
		Date listingDate = null; // 上市日期
		Date updateDate = null; // 更新日期
		int validity = 1; // 1有效 0无效
		int type = 1; // 1主力合约 2 非主力合约
		String pTicker = null; // 父期指

		// parameters in FuturesPosition table
		float nature = 0;

		// parameters in kchartsday table
		BigDecimal closePrice = BigDecimal.ZERO; // 收盘价格
		BigDecimal amount = BigDecimal.ZERO; // 累计成交金额

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
			PreparedStatement positionPrestmt = conn
					.prepareStatement(positionSql);

			IndexDetailsUtil indexDetailsUtil = new IndexDetailsUtil(conn);
			IndextPropertyUtil indextPropertyUtil = new IndextPropertyUtil(conn);
			FuturesPositionUtil futuresPositionUtil = new FuturesPositionUtil(
					conn);

			if (firstMainTickerOperation) {
				mainTickerSet = indextPropertyUtil.getMainTicker();
				firstMainTickerOperation = false;
			}

			while (resultSet.next()) {
				// parameters in IndexDetails talbe
				stockCode = resultSet.getString("ContractId");
				currentPrice = resultSet.getBigDecimal("LatestPrice");
				yesterdaySettle = resultSet.getBigDecimal("PreSettlementPrice");
				hightAndLow = indexDetailsUtil.calHightAndLow(currentPrice,
						yesterdaySettle);
				hightAndLowRange = indexDetailsUtil.calHightAndLowRange(
						currentPrice, yesterdaySettle);
				basis = indexDetailsUtil.calBasis(stockCode, currentPrice);
				position = BigInteger.valueOf(resultSet.getLong("Holdings"));
				BigInteger preHoldings = BigInteger.valueOf(resultSet.getLong("PreHoldings"));
				dayLoadingUp = indexDetailsUtil.calDayLoadingUp(position,
						preHoldings);
				datetime = resultSet.getTimestamp("TradingTime");
				openPrice = resultSet.getBigDecimal("CurrOpenPrice");
				volumnCount = BigInteger.valueOf(resultSet.getLong("Volume"));
				nowHand = indexDetailsUtil.calNowHand(stockCode, datetime,
						volumnCount);
				turnover = resultSet.getBigDecimal("Turnover");
				highPrice = resultSet.getBigDecimal("TopPrice");
				lowPrice = resultSet.getBigDecimal("BottomPrice");
				loadingUp = indexDetailsUtil.calLoadingUp(stockCode, datetime,
						position);
				estimationSettle = resultSet
						.getBigDecimal("CurrSettlementPrice");
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

				// parameters in FuturesPosition table
				nature = futuresPositionUtil.calNature();

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
				detailsPrestmt.setBigDecimal(
						IndexDetails.CurrentPrice.ordinal() + 1, currentPrice);
				detailsPrestmt.setFloat(IndexDetails.HightAndLow.ordinal() + 1,
						hightAndLow);
				detailsPrestmt.setFloat(
						IndexDetails.HightAndLowRange.ordinal() + 1,
						hightAndLowRange);
				detailsPrestmt
						.setFloat(IndexDetails.Basis.ordinal() + 1, basis);
				detailsPrestmt.setLong(IndexDetails.DayLoadingUp.ordinal() + 1,
						dayLoadingUp.longValue());
				detailsPrestmt.setTimestamp(
						IndexDetails.Datetime.ordinal() + 1, datetime);
				detailsPrestmt.setBigDecimal(
						IndexDetails.YesterdaySettle.ordinal() + 1,
						yesterdaySettle);
				detailsPrestmt.setBigDecimal(
						IndexDetails.OpenPrice.ordinal() + 1, openPrice);
				detailsPrestmt.setLong(IndexDetails.NowHand.ordinal() + 1,
						nowHand.longValue());
				detailsPrestmt.setLong(IndexDetails.VolumnCount.ordinal() + 1,
						volumnCount.longValue());
				detailsPrestmt.setBigDecimal(
						IndexDetails.Turnover.ordinal() + 1, turnover);
				detailsPrestmt.setBigDecimal(
						IndexDetails.HighPrice.ordinal() + 1, highPrice);
				detailsPrestmt.setBigDecimal(
						IndexDetails.LowPrice.ordinal() + 1, lowPrice);
				detailsPrestmt.setLong(IndexDetails.Position.ordinal() + 1,
						position.longValue());
				detailsPrestmt.setLong(IndexDetails.LoadingUp.ordinal() + 1,
						loadingUp.longValue());
				detailsPrestmt.setBigDecimal(
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

				// set the values in futuresposition table
				positionPrestmt.setString(FuturesPosition.Ticker.ordinal() + 1,
						stockCode);
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
				kChartsDayPrestmt.setString(KChartsDay.Ticker.ordinal() + 1,
						stockCode);
				kChartsDayPrestmt.setTimestamp(
						KChartsDay.Datetime.ordinal() + 1, datetime);
				kChartsDayPrestmt.setBigDecimal(
						KChartsDay.OpenPrice.ordinal() + 1, openPrice);
				kChartsDayPrestmt.setBigDecimal(
						KChartsDay.HightPrice.ordinal() + 1, highPrice);
				kChartsDayPrestmt.setBigDecimal(
						KChartsDay.LowPrice.ordinal() + 1, lowPrice);
				kChartsDayPrestmt.setBigDecimal(
						KChartsDay.ClosePrice.ordinal() + 1, closePrice);
				kChartsDayPrestmt.setLong(KChartsDay.Volume.ordinal() + 1,
						volumnCount.longValue());
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
