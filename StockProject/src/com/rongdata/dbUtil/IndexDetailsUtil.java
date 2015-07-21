package com.rongdata.dbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

enum IndexDetails {
	StockCode, CurrentPrice, HightAndLow, HightAndLowRange, Basis, DayLoadingUp, Datetime, 
	YesterdaySettle, OpenPrice, NowHand, VolumnCount, Turnover, HighPrice, LowPrice, Position, LoadingUp, 
	EstimationSettle, TradingSentiment, WindVane, MassOfPublicOpinion;
}

public class IndexDetailsUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;

	public IndexDetailsUtil() {
		// TODO Auto-generated constructor stub
	}

	public IndexDetailsUtil(Connection conn) {
		this.conn = conn;
	}

	public IndexDetailsUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public IndexDetailsUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}

	/**
	 * for IC,IF,IH
	 */
	public void insertAll() {
		String targetSql = "insert ignore into xcube.com_index_details(StockCode, CurrentPrice, "
				+ "HightAndLow, HightAndLowRange, Basis, DayLoadingUp, Datetime,YesterdaySettle, "
				+ "OpenPrice, NowHand, VolumnCount, HighPrice, LowPrice, Position, LoadingUp, "
				+ "EstimationSettle, TradingSentiment, WindVane, MassOfPublicOpinion) "
				+ "value(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		String sourceSql = "select contractid, latestprice, presettlementprice, holdings, "
				+ "preholdings, tradingtime, CurrOpenPrice, Volume, TopPrice, BottomPrice, "
				+ "CurrSettlementPrice from (select * from xcube.market_quotation "
				+ "where contractid rlike '^I.*' order by tradingtime<=now(),"
				+ "tradingtime desc) as a group by contractid;";

		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}

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
		float highPrice = 0; // 最高价
		float lowPrice = 0; // 最低价
		float position = 0; // 持仓
		float loadingUp = 0; // 增仓
		float estimationSettle = 0; // 估结算
		float tradingSentiment = 0; // 交易情绪
		float windVane = 0; // 风向标
		float massOfPublicOpinion = 0; // 大众舆情

		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);

			while (resultSet.next()) {
				stockCode = resultSet.getString("ContractId");
				currentPrice = resultSet.getFloat("LatestPrice");
				yesterdaySettle = resultSet.getFloat("PreSettlementPrice");
				hightAndLow = calHightAndLow(currentPrice, yesterdaySettle);
				hightAndLowRange = calHightAndLowRange(currentPrice,
						yesterdaySettle);
				basis = calBasis(stockCode, currentPrice);
				position = resultSet.getFloat("Holdings");

				float preHoldings = resultSet.getFloat("PreHoldings");
				dayLoadingUp = calDayLoadingUp(position, preHoldings);
				datetime = resultSet.getTimestamp("TradingTime");
				openPrice = resultSet.getFloat("CurrOpenPrice");
				volumnCount = resultSet.getFloat("Volume");
				nowHand = calNowHand(stockCode, datetime, volumnCount);
				highPrice = resultSet.getFloat("TopPrice");
				lowPrice = resultSet.getFloat("BottomPrice");
				loadingUp = calLoadingUp(stockCode, datetime, position);
				estimationSettle = resultSet.getFloat("CurrSettlementPrice");
				tradingSentiment = calTradingSentiment(stockCode, datetime);
				windVane = calWindVane();
				massOfPublicOpinion = calMassOfPublicOpinion();

				prestmt.setString(IndexDetails.StockCode.ordinal() + 1,
						stockCode);
				prestmt.setFloat(IndexDetails.CurrentPrice.ordinal() + 1,
						currentPrice);
				prestmt.setFloat(IndexDetails.HightAndLow.ordinal() + 1,
						hightAndLow);
				prestmt.setFloat(IndexDetails.HightAndLowRange.ordinal() + 1,
						hightAndLowRange);
				prestmt.setFloat(IndexDetails.Basis.ordinal() + 1, basis);
				prestmt.setFloat(IndexDetails.DayLoadingUp.ordinal() + 1,
						dayLoadingUp);
				prestmt.setTimestamp(IndexDetails.Datetime.ordinal() + 1,
						datetime);
				prestmt.setFloat(IndexDetails.YesterdaySettle.ordinal() + 1,
						yesterdaySettle);
				prestmt.setFloat(IndexDetails.OpenPrice.ordinal() + 1,
						openPrice);
				prestmt.setFloat(IndexDetails.NowHand.ordinal() + 1, nowHand);
				prestmt.setFloat(IndexDetails.VolumnCount.ordinal() + 1,
						volumnCount);
				prestmt.setFloat(IndexDetails.HighPrice.ordinal() + 1,
						highPrice);
				prestmt.setFloat(IndexDetails.LowPrice.ordinal() + 1, lowPrice);
				prestmt.setFloat(IndexDetails.Position.ordinal() + 1, position);
				prestmt.setFloat(IndexDetails.LoadingUp.ordinal() + 1,
						loadingUp);
				prestmt.setFloat(IndexDetails.EstimationSettle.ordinal() + 1,
						estimationSettle);
				prestmt.setFloat(IndexDetails.TradingSentiment.ordinal() + 1,
						tradingSentiment);
				prestmt.setFloat(IndexDetails.WindVane.ordinal() + 1, windVane);
				prestmt.setFloat(
						IndexDetails.MassOfPublicOpinion.ordinal() + 1,
						massOfPublicOpinion);

				prestmt.execute();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	float calHightAndLow(float currentPrice, float yesterdaySettle) {
		// TODO Auto-generated method stub
		return currentPrice - yesterdaySettle;
	}

	float calHightAndLowRange(float currentPrice, float yesterdaySettle) {
		// TODO Auto-generated method stub
		if (yesterdaySettle != 0) {
			return (currentPrice - yesterdaySettle) / yesterdaySettle;
		} else {
			return Float.MAX_VALUE;
		}
	}

	// to be completed, lack of real-time price
	float calBasis(String stockCode, float currentPrice) {
		// TODO Auto-generated method stub
		return 0;
	}

	float calDayLoadingUp(float position, float preHoldings) {
		// TODO Auto-generated method stub
		return position - preHoldings;
	}

	float calNowHand(String stockCode, Timestamp datetime,
			float curVolumnCount) {
		// TODO Auto-generated method stub
		float prevCumVolume = new RawDataAccess(conn).getPrevCumVolume(stockCode,
				datetime, "index");
		return curVolumnCount - prevCumVolume;
	}

	float calLoadingUp(String stockCode, Timestamp datetime,
			float position) {
		// TODO Auto-generated method stub
		float prevHoldings = new RawDataAccess(conn).getPrevHoldings(stockCode,
				datetime, "index");
		return position - prevHoldings;
	}

	float calTradingSentiment(String stockCode, Timestamp datetime) {
		// TODO Auto-generated method stub
		float tradingSentiment = new TradingSentimentAccess(conn).getTradingSentiment(
				stockCode, datetime);
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

}
