package com.scandilabs.framework.util;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * A threadsafe implementation of SimpleDateFormat. Utilizes java ThreadLocal
 * variables to ensure there is one instance of DateFormat per thread. To see
 * why this is useful, google 'SimpleDateFormat' and 'threadsafe'.
 * 
 * @author mkvalsvik
 * 
 */
public class ThreadSafeSimpleDateFormat extends DateFormat {

    private static final long serialVersionUID = 1L;
    private String formatString;
    private TimeZone timeZone;
    private boolean lenient;
    private Calendar calendar;
    private NumberFormat numberFormat;

    private DateFormat getThreadLocalDateFormat() {
        String key = String.format("format-hash:%d", formatString.hashCode());
        SimpleDateFormat df = (SimpleDateFormat) ThreadLocalUtils.get(key);
        if (df == null) {
            df = new SimpleDateFormat(this.formatString);
            if (this.timeZone != null) {
                df.setTimeZone(this.timeZone);
            }
            df.setLenient(this.lenient);
            if (this.calendar != null) {
                df.setCalendar(this.calendar);
            }
            if (this.numberFormat != null) {
                df.setNumberFormat(this.numberFormat);
            }
            ThreadLocalUtils.set(key, df);
        }
        return df;
    }

    public ThreadSafeSimpleDateFormat(String formatString) {
        this.formatString = formatString;
    }

    public ThreadSafeSimpleDateFormat(String formatString, TimeZone timeZone) {
        this.formatString = formatString;
        this.timeZone = timeZone;
    }

    public StringBuffer format(Date date, StringBuffer toAppendTo,
            FieldPosition fieldPosition) {
        return this.getThreadLocalDateFormat().format(date, toAppendTo,
                fieldPosition);
    }

    public Date parse(String source, ParsePosition pos) {
        return this.getThreadLocalDateFormat().parse(source, pos);
    }

    public void setTimeZone(TimeZone zone) {
        this.timeZone = zone;
    }

    public void setCalendar(Calendar newCalendar) {
        this.calendar = newCalendar;
    }

    public void setLenient(boolean lenient) {
        this.lenient = lenient;
    }

    public void setNumberFormat(NumberFormat newNumberFormat) {
        this.numberFormat = newNumberFormat;
    }

    public static ThreadSafeSimpleDateFormat ZULU_FORMAT = new ThreadSafeSimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss'Z'", TimeZone.getTimeZone("GMT"));
}
