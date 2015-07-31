package com.rongdata.main;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mysql.jdbc.Connection;
import com.rongdata.dbUtil.DebtUtil;
import com.rongdata.dbUtil.FuturesUtil;
import com.rongdata.dbUtil.IndexUtil;
import com.rongdata.dbUtil.KChartsMonthUtil;
import com.rongdata.dbUtil.KChartsWeekUtil;
import com.rongdata.dbUtil.MysqlDBUtil;
import com.rongdata.dbUtil.StockUtil;
import com.rongdata.dbUtil.TickerTimeshareUtil;

public class StockProject {
	static Logger logger = LogManager.getLogger();

	public static void main(String[] args) {
		Calendar calendar = Calendar.getInstance();
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		calendar.set(year, month, day + 1, 9, 16, 0);
		Date date = calendar.getTime();

		int period = 24 * 60 * 60 * 1000;
		Timer timer = new Timer();
		StockHandleTask stockHandleTask = new StockHandleTask();
		System.out.println("[SCHEDULE]stock project starting ...");
		System.out.println("[SCHEDULE]waiting for scheduling ...");
		logger.info("stock project starting...");
		timer.schedule(stockHandleTask, date, period);

	}

}

class StockHandleTask extends TimerTask {
	static Logger logger = LogManager.getLogger();

	protected volatile boolean futuresStatementCreated = false;
	protected volatile boolean debtStatementCreated = false;
	protected volatile boolean indexStatementCreated = false;
	protected volatile boolean stockStatementCreated = false;
	protected volatile boolean timeShareStatementCreated = false;
	protected volatile boolean kChartsWeekStatementCreated = false;
	protected volatile boolean kChartsMonthStatementCreated = false;

	@Override
	public void run() {
		Connection conn = MysqlDBUtil.getConnection();
		// TODO Auto-generated method stub
		FuturesUtil futuresUtil = new FuturesUtil(conn);
		DebtUtil debtUtil = new DebtUtil(conn);
		IndexUtil indexUtil = new IndexUtil(conn);
		StockUtil stockUtil = new StockUtil(conn);
		TickerTimeshareUtil tickerTimeshareUtil = new TickerTimeshareUtil(conn);
		KChartsWeekUtil kChartsWeekUtil = new KChartsWeekUtil(conn);
		KChartsMonthUtil kChartsMonthUtil = new KChartsMonthUtil(conn);

		// futuresThread
		System.out.println("futuresThread starting");
		logger.info("futuresThread starting");
		Thread futuresThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					System.out.println(">>>starting futures operate .....");
					futuresUtil.operate();
					System.out.println(">>>ended futures operate .....");
					if (!futuresStatementCreated) {
						futuresStatementCreated = true;
					}
					try {
						System.out.println(">>>sleeping for 5000 millis , current millistiem >>> "
								+ System.currentTimeMillis());
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (isNoonExpired(System.currentTimeMillis())) {
						Calendar calendar = Calendar.getInstance();
						long curMillis = calendar.getTimeInMillis();
						calendar.set(calendar.get(Calendar.YEAR),
								calendar.get(Calendar.MONTH),
								calendar.get(Calendar.DAY_OF_MONTH), 13, 0, 0);
						long expectedMillis = calendar.getTimeInMillis();
						System.out.println("Now is NoonExpired time, futuresThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");
						logger.info("Now is NoonExpired time, futuresThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");

						try {
							Thread.sleep(expectedMillis - curMillis);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				if (futuresStatementCreated) {
					System.out.println("close futures statement");
					futuresUtil.closeStatement();
				}
			}
		}, "FuturesThread");
		futuresThread.start();

		// debtThread
		System.out.println("debtThread starting");
		logger.info("debtThread starting");
		Thread debtThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					System.out.println(">>>starting debt operation .....");
					debtUtil.operate();
					System.out.println(">>>ended debt operation .....");
					if (!debtStatementCreated) {
						debtStatementCreated = true;
					}
					try {
						System.out.println(">>>sleeping for 5000 millis, currentMillisTime >>> "
								+ System.currentTimeMillis());
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (isNoonExpired(System.currentTimeMillis())) {
						Calendar calendar = Calendar.getInstance();
						long curMillis = calendar.getTimeInMillis();
						calendar.set(calendar.get(Calendar.YEAR),
								calendar.get(Calendar.MONTH),
								calendar.get(Calendar.DAY_OF_MONTH), 13, 0, 0);
						long expectedMillis = calendar.getTimeInMillis();
						System.out.println("Now is NoonExpired time, debtThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");
						logger.info("Now is NoonExpired time, debtThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");

						try {
							Thread.sleep(expectedMillis - curMillis);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				if (debtStatementCreated) {
					System.out.println("close debt statement");
					debtUtil.closeStatement();
				}
			}
		}, "DebtThread");
		debtThread.start();

		// indexThread
		System.out.println("indexThread starting");
		logger.info("indexThread starting");
		Thread indexThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					System.out.println(">>> starting index operation");
					indexUtil.operate();
					System.out.println(">>> ended index operation");
					if (!indexStatementCreated) {
						indexStatementCreated = true;
					}
					try {
						System.out.println(">>> index sleeping for 5000 millis, currentMillisTime >>> "
								+ System.currentTimeMillis());
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (isNoonExpired(System.currentTimeMillis())) {
						Calendar calendar = Calendar.getInstance();
						long curMillis = calendar.getTimeInMillis();
						calendar.set(calendar.get(Calendar.YEAR),
								calendar.get(Calendar.MONTH),
								calendar.get(Calendar.DAY_OF_MONTH), 13, 0, 0);
						long expectedMillis = calendar.getTimeInMillis();
						System.out.println("Now is NoonExpired time, indexThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");
						logger.info("Now is NoonExpired time, indexThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");

						try {
							Thread.sleep(expectedMillis - curMillis);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				if (indexStatementCreated) {
					System.out.println("close index statement");
					indexUtil.closeStatement();
				}
			}
		}, "IndexThread");
		indexThread.start();

		// stockThread
		System.out.println("stockThread starting");
		logger.info("stockThread starting");
		Thread stockThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					System.out.println(">>> getting stock operation");
					stockUtil.operate();
					System.out.println(">>> ended stock operation");
					if (!stockStatementCreated) {
						stockStatementCreated = true;
					}
					try {
						System.out.println("stock sleeping for 5000 millis, currentMillisTime >>> "
								+ System.currentTimeMillis());
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (isNoonExpired(System.currentTimeMillis())) {
						Calendar calendar = Calendar.getInstance();
						long curMillis = calendar.getTimeInMillis();
						calendar.set(calendar.get(Calendar.YEAR),
								calendar.get(Calendar.MONTH),
								calendar.get(Calendar.DAY_OF_MONTH), 13, 0, 0);
						long expectedMillis = calendar.getTimeInMillis();
						System.out.println("Now is NoonExpired time, stockThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");
						logger.info("Now is NoonExpired time, stockThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");

						try {
							Thread.sleep(expectedMillis - curMillis);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				if (stockStatementCreated) {
					System.out.println("close stock statement");
					stockUtil.closeStatement();
				}
			}
		}, "StockThread");
		stockThread.start();

		// tickerTimeShareThread
		System.out.println("tickerTimeShareThread starting");
		logger.info("tickerTimeShareThread starting");
		Thread timeShareThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					System.out.println(">>> TickerTimeShare Starting Operation");
					tickerTimeshareUtil.insertAll();
					System.out.println(">>> TickerTimeShare Ended Operation");
					if (!timeShareStatementCreated) {
						timeShareStatementCreated = true;
					}
					try {
						System.out
								.println(">>> TickerTimeShare Sleeping For 5000 Millis, currentMillisTime >>> "
										+ System.currentTimeMillis());
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (isNoonExpired(System.currentTimeMillis())) {
						Calendar calendar = Calendar.getInstance();
						long curMillis = calendar.getTimeInMillis();
						calendar.set(calendar.get(Calendar.YEAR),
								calendar.get(Calendar.MONTH),
								calendar.get(Calendar.DAY_OF_MONTH), 13, 0, 0);
						long expectedMillis = calendar.getTimeInMillis();
						System.out
								.println("Now is NoonExpired time, tickerTimeShareThread sleeping for "
										+ (expectedMillis - curMillis)
										+ " millis.");
						logger.info("Now is NoonExpired time, tickerTimeShareThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");

						try {
							Thread.sleep(expectedMillis - curMillis);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				if (timeShareStatementCreated) {
					System.out.println("close tickertimeshare statement");
					tickerTimeshareUtil.closeStatement();
				}
			}
		}, "TickerTimeShareThread");
		timeShareThread.start();

		// kChartsWeekThread
		System.out.println("kChartsWeekThread starting");
		logger.info("kChartsWeekThread starting");
		Thread kChartsWeekThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					System.out.println(">>> KChartsWeek Starting Operation");
					kChartsWeekUtil.insertAll();
					System.out.println(">>> KChartsWeek Ended Operation");
					if (!kChartsWeekStatementCreated) {
						kChartsMonthStatementCreated = true;
					}
					try {
						System.out.println(">>> KChartsWeek Sleeping For 5000 Millis, currentMillisTime >>> "
								+ System.currentTimeMillis());
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (isNoonExpired(System.currentTimeMillis())) {
						Calendar calendar = Calendar.getInstance();
						long curMillis = calendar.getTimeInMillis();
						calendar.set(calendar.get(Calendar.YEAR),
								calendar.get(Calendar.MONTH),
								calendar.get(Calendar.DAY_OF_MONTH), 13, 0, 0);
						long expectedMillis = calendar.getTimeInMillis();
						System.out.println("Now is NoonExpired time, kChartsWeekThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");
						logger.info("Now is NoonExpired time, kChartsWeekThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");

						try {
							Thread.sleep(expectedMillis - curMillis);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				if (kChartsWeekStatementCreated) {
					System.out.println("close kchartsweek statement");
					kChartsWeekUtil.closeStatement();
				}
			}
		}, "KChartsWeekThread");
		kChartsWeekThread.start();

		// kChartsMonthThread
		System.out.println("kChartsMonthThread starting");
		logger.info("kChartsMonthThread starting");
		Thread kChartsMonthThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					System.out.println(">>> KChartsMonth Starting Operation");
					kChartsMonthUtil.insertAll();
					System.out.println(">>> KChartsMonth Ended Operation");
					if (!kChartsMonthStatementCreated) {
						kChartsMonthStatementCreated = true;
					}
					try {
						System.out.println("KChartsMonth Sleeping For 5000 Millis, currentMillisTime >>> "
								+ System.currentTimeMillis());
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (isNoonExpired(System.currentTimeMillis())) {
						Calendar calendar = Calendar.getInstance();
						long curMillis = calendar.getTimeInMillis();
						calendar.set(calendar.get(Calendar.YEAR),
								calendar.get(Calendar.MONTH),
								calendar.get(Calendar.DAY_OF_MONTH), 13, 0, 0);
						long expectedMillis = calendar.getTimeInMillis();
						System.out.println("Now is NoonExpired time, kChartsMonthThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");
						logger.info("Now is NoonExpired time, kChartsMonthThread sleeping for "
								+ (expectedMillis - curMillis) + " millis.");

						try {
							Thread.sleep(expectedMillis - curMillis);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				if (kChartsMonthStatementCreated) {
					System.out.println("close kchartsmonth statement");
					kChartsMonthUtil.closeStatement();
				}
			}
		}, "KChartsMonthThread");
		kChartsMonthThread.start();

		logger.info("close preparedStatement");

		// connCloseOperation
		System.out.println("connCloseOperation starting");
		logger.info("connCloseOperation");
		Thread connCloseOperation = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (true) {
					if (allStatementsClosed()) {
						try {
							System.out.println("close connection");
							logger.info("close connection");
							conn.close();
							break;
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}, "connCloseOperationThread");
		connCloseOperation.start();
	}

	private boolean allStatementsClosed() {
		// TODO Auto-generated method stub
		if (futuresStatementCreated && debtStatementCreated
				&& indexStatementCreated && stockStatementCreated
				&& timeShareStatementCreated && kChartsWeekStatementCreated
				&& kChartsMonthStatementCreated) {
			return true;
		}
		return false;
	}

	protected boolean isNoonExpired(long currentTimeMillis) {
		// TODO Auto-generated method stub
		if (currentTimeMillis == 0) {
			System.out.println("noon calendar is null");
			return false;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(currentTimeMillis);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		if ((hour == 11 && minute > 32) || (hour > 11 && hour < 13)) {
			System.out.println("NoonExpired >>> true");
			return true;
		}
		System.out.println("NoonExpired >>> false");
		return false;
	}

	private boolean isAfterExpired(long currentTimeMillis) {
		// TODO Auto-generated method stub
		if (currentTimeMillis == 0) {
			System.out.println("afternoon calendar is null");
			return false;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(currentTimeMillis);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		if ((hour == 15 && minute > 17) || (hour > 15)) {
			System.out.println("afterexpired >>> true");
			return true;
		}
		System.out.println("afterexpired >>> false");
		return false;
	}

}
