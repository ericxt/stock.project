package com.rongdata.dbUtil;

import java.math.BigDecimal;
import java.math.BigInteger;

public class Test {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String str = "123445678786968689";
		BigInteger bigInteger = new BigInteger(str);
		System.out.println(bigInteger);
//		BigDecimal bigDecimal = new BigDecimal(str);
//		bigDecimal = bigDecimal.setScale(1, BigDecimal.ROUND_HALF_DOWN);
//		System.out.println(bigDecimal);
	}
}
