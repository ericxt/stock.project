package com.rongdata.dbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.mysql.jdbc.Connection;

enum KChartsDay {
	Ticker, Datetime, OpenPrice, HightPrice, LowPrice, ClosePrice, 
	Volume, Amount, TradingSentiment, WindVane, MassOfPublicOpinion;
}

public class KChartsDayUtil {
	private Connection conn = null;
	private ResultSet resultSet = null;
	
	public KChartsDayUtil() {
		// TODO Auto-generated constructor stub
	}
	
	public KChartsDayUtil(Connection conn) {
		this.conn = conn;
	}
	
	public KChartsDayUtil(ResultSet resultSet) {
		this.resultSet = resultSet;
	}
	
	public KChartsDayUtil(Connection conn, ResultSet resultSet) {
		this.conn = conn;
		this.resultSet = resultSet;
	}
	
	// 日K线：以每日最高、最低、开盘、收盘为元素，即每个“蜡烛”对应一天
	// 仅存储连续两个月的数据，过渡到第三个月，则删掉第一个月的数据。方便计算跨月时，周K线计算
	public void insertAll() {
		String targetSql = "insert ignore into xcube.com_k_charts_day(Ticker, Datetime, "
				+ "OpenPrice, HightPrice, LowPrice, ClosePrice, Volume, Amount, "
				+ "TradingSentiment, WindVane, MassOfPublicOpinion) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
		// use latestprice to update the closeprice
		String sourceSql = "select ContractId, TradingTime, CurrOpenPrice, TopPrice, "
				+ "BottomPrice, LatestPrice, Volume, TurnOver from "
				+ "(select ContractId, TradingTime, CurrOpenPrice, TopPrice, "
				+ "BottomPrice, LatestPrice, Volume, TurnOver from xcube.market_quotation "
				+ "order by TradingTime<=now(),TradingTime desc) as a group by ContractId;";
		
		String ticker = null;	// 证券代码
		Timestamp datetime = null;	// 时间
		float openPrice = 0;	// 开盘价
		float hightPrice = 0;	// 最高价格
		float lowPrice = 0;	// 最低价格
		float closePrice = 0;	// 收盘价格
		float volume = 0;	// 累计成交量
		float amount = 0;	// 累计成交金额
		float tradingSentiment = 0;	// 交易情绪
		float windVane = 0;	// 风向标
		float massOfPublicOpinion = 0;	// 大众舆情
		
		if (conn == null) {
			conn = MysqlDBUtil.getConnection();
		}
		if (resultSet == null) {
			resultSet = new RawDataAccess(conn).getRawData(sourceSql);
		}
		
		try {
			PreparedStatement prestmt = conn.prepareStatement(targetSql);
			
			while (resultSet.next()) {
				ticker = resultSet.getString("ContractId");
				datetime = resultSet.getTimestamp("TradingTime");
				openPrice = resultSet.getFloat("CurrOpenPrice");
				hightPrice = resultSet.getFloat("TopPrice");
				lowPrice = resultSet.getFloat("BottomPrice");
				closePrice = resultSet.getFloat("LatestPrice");
				volume = resultSet.getFloat("Volume");
				amount = resultSet.getFloat("TurnOver");
				tradingSentiment = calTradingSentiment(ticker, datetime);
				windVane = calWindVane();
				massOfPublicOpinion = calMassOfPublicOpinion();
				
				prestmt.setString(KChartsDay.Ticker.ordinal() + 1, ticker);
				prestmt.setTimestamp(KChartsDay.Datetime.ordinal() + 1, datetime);
				prestmt.setFloat(KChartsDay.OpenPrice.ordinal() + 1, openPrice);
				prestmt.setFloat(KChartsDay.HightPrice.ordinal() + 1, hightPrice);
				prestmt.setFloat(KChartsDay.LowPrice.ordinal() + 1, lowPrice);
				prestmt.setFloat(KChartsDay.ClosePrice.ordinal() + 1, closePrice);
				prestmt.setFloat(KChartsDay.Volume.ordinal() + 1, volume);
				prestmt.setFloat(KChartsDay.Amount.ordinal() + 1, amount);
				prestmt.setFloat(KChartsDay.TradingSentiment.ordinal() + 1, tradingSentiment);
				prestmt.setFloat(KChartsDay.WindVane.ordinal() + 1, windVane);
				prestmt.setFloat(KChartsDay.MassOfPublicOpinion.ordinal() + 1, massOfPublicOpinion);
				
				prestmt.execute();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// to be completed
	private float calMassOfPublicOpinion() {
		// TODO Auto-generated method stub
		return 0;
	}

	// to be completed
	private float calWindVane() {
		// TODO Auto-generated method stub
		return 0;
	}

	private float calTradingSentiment(String ticker, Timestamp datetime) {
		// TODO Auto-generated method stub
		float tradingSentiment = new TradingSentimentAccess(conn).getTradingSentiment(ticker, datetime);
		return tradingSentiment;
	}

}
