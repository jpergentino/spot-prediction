package core.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DateUtil {
	
	public static final String DATE_PATTERN = "yyyy/MM/dd";
	public static final String DATETIME_WITHOUT_MILLISECOND_PATTERN_TO_FILE = "yyyy_MM_dd_HH_mm_ss";
	public static final String DATETIME_WITHOUT_MILLISECOND_PATTERN = "yyyy/MM/dd HH:mm:ss";
	public static final String DATETIME_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS";
	private static final Logger log = LogManager.getLogger(DateUtil.class);
	
	private static final SimpleDateFormat sdfDatetime = new SimpleDateFormat(DATETIME_WITHOUT_MILLISECOND_PATTERN);
	private static final SimpleDateFormat sdfDatetimeToFile = new SimpleDateFormat(DATETIME_WITHOUT_MILLISECOND_PATTERN_TO_FILE);
	
	
	public static String getFormattedDateTimeToFile() {
		return sdfDatetimeToFile.format(new Date(System.currentTimeMillis()));
	}
	
	public static String getFormattedDateTimeToFile(Date d) {
		return d != null ? sdfDatetimeToFile.format(d) : null;
	}
	
	public static String getFormattedDateTime(Date d) {
		return d != null ? sdfDatetime.format(d) : null;
	}
	
	/**
	 * Return an date.
	 * @param formattedDate as yyyy/MM/dd
	 * @return Return an date.
	 */
	public static final Date getDate(String formattedDate) {
		Date ret = null;
		try {
			ret = new SimpleDateFormat(DATE_PATTERN).parse(formattedDate);
		} catch (ParseException e) {
			log.error("Problem to parse date: "+ formattedDate);
		}
		return ret;
	}
	
	/**
	 * Return an date.
	 * @param formattedDate as yyyy/MM/dd hh:mm:ss.SSS
	 * @return Return an date.
	 */
	public static final Date getDateTime(String formattedDate) {
		Date ret = null;
		try {
			ret = new SimpleDateFormat(DATETIME_PATTERN).parse(formattedDate);
		} catch (ParseException e) {
			log.error("Problem to parse date: "+ formattedDate);
		}
		return ret;
	}
	
	/**
	 * Return an date with 0 hour.
	 * @param formattedDate as yyyy/MM/dd
	 * @return Return an date with 0 hour.
	 */
	public static final Date getInitDate(String formattedDate) {
		Date ret = null;
		try {
			ret = getInitDate(new SimpleDateFormat(DATE_PATTERN).parse(formattedDate).getTime());
		} catch (ParseException e) {
			log.error("Problem to parse date: "+ formattedDate);
		}
		return ret;
	}
		
	/**
	 * Return an date with 0 hour.
	 * @param datetime as timestamp.
	 * @return Return an date with 0 hour.
	 */
	public static final Date getInitDate(Long datetime) {
		Calendar ret = Calendar.getInstance();
		ret.setTimeInMillis(datetime);
		return getInitDate(ret.getTime());
	}
	
	/**
	 * Return an date with 0 hour.
	 * @param paramDate as Date.
	 * @return Return the same date with 0 hour.
	 */
	public static final Date getInitDate(Date paramDate) {
		
		Calendar ret = Calendar.getInstance();
		ret.setTimeInMillis(paramDate.getTime());
		ret.set(Calendar.HOUR_OF_DAY, 0);
		ret.set(Calendar.MINUTE, 0);
		ret.set(Calendar.SECOND, 0);
		ret.set(Calendar.MILLISECOND, 0);
		
		return ret.getTime();
	}

	
	/**
	 * Return an date with 23:59 hour.
	 * @param formattedDate as yyyy/MM/dd
	 * @return Return an date with 23:59 hour.
	 */
	public static final Date getEndOfDate(String formattedDate) {
		Date ret = null;
		try {
			ret = getEndOfDate(new SimpleDateFormat(DATE_PATTERN).parse(formattedDate).getTime());
		} catch (ParseException e) {
			log.error("Problem to parse date: "+ formattedDate);
		}
		return ret;
	}
		
	/**
	 * Return an date with 23:59 hour.
	 * @param datetime as timestamp.
	 * @return Return an date with 23:59 hour.
	 */
	public static final Date getEndOfDate(Long datetime) {
		Calendar ret = Calendar.getInstance();
		ret.setTimeInMillis(datetime);
		return getInitDate(ret.getTime());
	}
	
	
	/**
	 * Return an date with 23:59 hour.
	 * @param paramDate as Date.
	 * @return Return the same date with 0 hour.
	 */
	public static final Date getEndOfDate(Date paramDate) {
		
		Calendar ret = Calendar.getInstance();
		ret.setTimeInMillis(paramDate.getTime());
		ret.set(Calendar.HOUR_OF_DAY, 23);
		ret.set(Calendar.MINUTE, 59);
		ret.set(Calendar.SECOND, 59);
		ret.set(Calendar.MILLISECOND, 999);
		
		return ret.getTime();
	}
	
}
