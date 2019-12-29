package com.cmccstormjk02.tools;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateFmt {

	public static final String date_long = "yyyy-MM-dd HH:mm:ss";
	public static final String date_short = "yyyy-MM-dd";
	public static final String date_minute = "yyyyMMddHHmm";

	public static SimpleDateFormat sdf = new SimpleDateFormat(date_short);



    public static String getCountDate(String date, String pattern) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Calendar cal = Calendar.getInstance();
		if (date != null) {
			try {
				// 2016-01-05 10:25:17
				cal.setTime(sdf.parse(date));

			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		return sdf.format(cal.getTime());
	}

	public static String getCountDate(String date, String pattern, int step) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Calendar cal = Calendar.getInstance();
		if (date != null) {
			try {
				cal.setTime(sdf.parse(date));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		cal.add(Calendar.DAY_OF_MONTH, step);
		return sdf.format(cal.getTime());
	}

	public static Date parseDate(String dateStr) throws Exception {
		return sdf.parse(dateStr);
	}

	public static void main(String[] args) throws Exception {

		System.out.println(DateFmt.getCountDate("2016-01-05 10:25:17", DateFmt.date_short));
        System.out.println(DateFmt.getCountDate(null, DateFmt.date_short));
        System.out.println(DateFmt.getCountDate(null, DateFmt.date_minute));
    }

}
