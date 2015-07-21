package com.rongdata.main;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rongdata.dbUtil.DebtUtil;
import com.rongdata.dbUtil.FuturesUtil;
import com.rongdata.dbUtil.IndexUtil;
import com.rongdata.dbUtil.KChartsMonthUtil;
import com.rongdata.dbUtil.KChartsWeekUtil;
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
		System.out.println("stock project starting ...");
		System.out.println("waiting for scheduling ...");
		logger.info("stock project starting...");
		timer.schedule(stockHandleTask, date, period);

	}

}

class StockHandleTask extends TimerTask {
	static Logger logger = LogManager.getLogger();

	@Override
	public void run() {
		// TODO Auto-generated method stub
		FuturesUtil futuresUtil = new FuturesUtil();
		DebtUtil debtUtil = new DebtUtil();
		IndexUtil indexUtil = new IndexUtil();
		StockUtil stockUtil = new StockUtil();
		TickerTimeshareUtil tickerTimeshareUtil = new TickerTimeshareUtil();
		KChartsWeekUtil kChartsWeekUtil = new KChartsWeekUtil();
		KChartsMonthUtil kChartsMonthUtil = new KChartsMonthUtil();

		// futuresThread
		System.out.println("futuresThread starting");
		logger.info("futuresThread starting");
		Thread futuresThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					futuresUtil.operate();
					try {
						Thread.sleep(500);
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
								.println("Now is NoonExpired time, futuresThread sleeping for "
										+ (expectedMillis - curMillis)
										+ " millis.");
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
			}
		});
		futuresThread.start();

		// debtThread
		System.out.println("debtThread starting");
		logger.info("debtThread starting");
		Thread debtThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					debtUtil.operate();
					try {
						Thread.sleep(500);
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
								.println("Now is NoonExpired time, debtThread sleeping for "
										+ (expectedMillis - curMillis)
										+ " millis.");
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

			}
		});
		debtThread.start();

		// indexThread
		System.out.println("indexThread starting");
		logger.info("indexThread starting");
		Thread indexThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					indexUtil.operate();
					try {
						Thread.sleep(500);
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
								.println("Now is NoonExpired time, indexThread sleeping for "
										+ (expectedMillis - curMillis)
										+ " millis.");
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
			}
		});
		indexThread.start();

		// stockThread
		System.out.println("stockThread starting");
		logger.info("stockThread starting");
		Thread stockThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					stockUtil.operate();
					try {
						Thread.sleep(500);
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
								.println("Now is NoonExpired time, stockThread sleeping for "
										+ (expectedMillis - curMillis)
										+ " millis.");
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
			}
		});
		stockThread.start();

		// tickerTimeShareThread
		System.out.println("tickerTimeShareThread starting");
		logger.info("tickerTimeShareThread starting");
		Thread timeShareThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					tickerTimeshareUtil.insertAll();
					try {
						Thread.sleep(500);
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
			}
		});
		timeShareThread.start();

		// kChartsWeekThread
		System.out.println("kChartsWeekThread starting");
		logger.info("kChartsWeekThread starting");
		Thread kChartsWeekThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					kChartsWeekUtil.insertAll();
					try {
						Thread.sleep(500);
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
								.println("Now is NoonExpired time, kChartsWeekThread sleeping for "
										+ (expectedMillis - curMillis)
										+ " millis.");
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
			}
		});
		kChartsWeekThread.start();

		// kChartsMonthThread
		System.out.println("kChartsMonthThread starting");
		logger.info("kChartsMonthThread starting");
		Thread kChartsMonthThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while (!isAfterExpired(System.currentTimeMillis())) {
					kChartsMonthUtil.insertAll();
					try {
						Thread.sleep(500);
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
								.println("Now is NoonExpired time, kChartsMonthThread sleeping for "
										+ (expectedMillis - curMillis)
										+ " millis.");
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
			}
		});
		kChartsMonthThread.start();
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
