package com.rongdata.dbUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;

public class Test {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HashMap<String,ArrayList<BigDecimal>> hashMap = new HashMap<String, ArrayList<BigDecimal>>();
		ArrayList<BigDecimal> list = new ArrayList<BigDecimal>();
		list.add(BigDecimal.valueOf(1242));
		list.add(BigDecimal.valueOf(4567));
		hashMap.put("ta233", list);
		ArrayList<BigDecimal> lista = new ArrayList<BigDecimal>();
		lista.add(BigDecimal.valueOf(123));
		lista.add(BigDecimal.valueOf(89));
		hashMap.put("t768", lista);
		
		System.out.println(hashMap.get("ta233") + ", " + hashMap.get("t768"));
	}
}
