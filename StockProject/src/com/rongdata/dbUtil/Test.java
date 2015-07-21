package com.rongdata.dbUtil;

import java.util.Calendar;

public class Test {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Calendar calendar = Calendar.getInstance();
		int dayofmonth = calendar.get(Calendar.DAY_OF_MONTH);
		System.out.println(dayofmonth);
		calendar.set(Calendar.DAY_OF_MONTH, 4);
		int dayofweek = calendar.get(Calendar.DAY_OF_WEEK);
		if (dayofweek == 0) {
			calendar.set(Calendar.DAY_OF_MONTH, 2);
		} else if (dayofweek == 7) {
			calendar.set(Calendar.DAY_OF_MONTH, 3);
		}
		System.out.println(calendar.getTime());
	}
}
