package com.linemetrics.monk.helper;

import com.linemetrics.monk.dao.DataItem;
import com.linemetrics.monk.director.RunnerContext;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TemplateParser {

    private static final String fieldStart = "\\$\\{";
    private static final String fieldEnd = "\\}";

    private static final String regex = fieldStart + "([^}]+)" + fieldEnd;
    private static final Pattern pattern = Pattern.compile(regex);

    private static final String regexPlaceholder = "([a-zA-Z-_]+).([a-zA-Z-_]+)(:([^}]+)){0,1}";
    private static final Pattern patternPlaceholder = Pattern.compile(regexPlaceholder);

    private static Map<String, DecimalFormat> decimalFormatCache = new HashMap<>();
    private static Map<String, SimpleDateFormat> dateFormatCache = new HashMap<>();
    private static Map<String, Locale> localeCache               = new HashMap<>();
    private static Map<String, TimeZone> timeZoneCache           = new HashMap<>();

    public static String parse(
        String template,
        String locale,
        Map<String, String> metaInfos,
        RunnerContext ctx,
        DataItem item) {

//        System.out.println(template);
        Matcher m = pattern.matcher(template);
        String result = template;

        while (m.find()) {

//            System.out.println(m.group(1));

            Matcher ph = patternPlaceholder.matcher(m.group(1));

            if( ! ph.find()) {
                result = result.replaceFirst(regex, "");
                continue;
            }

            String ph_ctx = ph.group(1);
            String ph_var = ph.group(2);
            String ph_format = ph.group(4);

            result = result.replaceFirst(
                regex,
                getVariable(
                    ph_ctx, ph_var, ph_format,
                    locale, metaInfos, ctx, item));
        }

//        System.out.println(result);

        return result;
    }

    private static String getVariable(
        String ctx,    String var,
        String format, String locale,
        Map<String, String> metaInfos,
        RunnerContext runnerCtx,
        DataItem item) {

        String tz = runnerCtx.getTimezone();

        switch(ctx) {
            case "item":
                if(item == null) {
                    break;
                }
                switch(var) {
                    case "start":   return formatDate(item.getTimestampStart(), format, tz);
                    case "end":     return formatDate(item.getTimestampEnd(), format, tz);
                    case "min":     return formatNumber(item.getMin(), format, locale);
                    case "max":     return formatNumber(item.getMax(), format, locale);
                    case "value":   return formatNumber(item.getValue(), format, locale);
                }
                break;
            case "meta":
                if(metaInfos != null && metaInfos.containsKey(var)) {
                    return metaInfos.get(var);
                }
                break;
            case "job":
                switch(var) {
                    case "start":       return formatDate(runnerCtx.getTimeFrom(), format, tz);
                    case "end":         return formatDate(runnerCtx.getTimeTo(), format, tz);
                    case "timezone":    return tz;
                }
                break;
        }
        return "UNDEFINED";
    }

    private static String formatDate(Long ts, String format, String locale) {
        if(ts != null){
            return dateFormat(format, locale).format(new Date(ts));
        }
        return null;
    }

    private static SimpleDateFormat dateFormat(String format, String timeZone) {
        String dateFormatCacheKey = String.format("%s_%s", timeZone, format);
        if( ! dateFormatCache.containsKey(dateFormatCacheKey)) {
            SimpleDateFormat dtf = new SimpleDateFormat(format);
            dtf.setTimeZone(TimeZone.getTimeZone(timeZone));
            dateFormatCache.put(dateFormatCacheKey, dtf);
        }
        return dateFormatCache.get(dateFormatCacheKey);
    }

    private static String formatNumber(Number number, String format, String locale) {
        return decimalFormat(format, locale).format(number);
    }

    private static DecimalFormat decimalFormat(String format, String locale) {
        String decimalFormatCacheKey = String.format("%s_%s", locale, format);
        if( ! decimalFormatCache.containsKey(decimalFormatCacheKey)) {
            DecimalFormat df = (DecimalFormat) NumberFormat.getNumberInstance(locale(locale));
            df.applyPattern(format);
            decimalFormatCache.put(decimalFormatCacheKey, df);
        }
        return decimalFormatCache.get(decimalFormatCacheKey);
    }

    private static Locale locale(String locale) {
        if( ! localeCache.containsKey(locale)) {
            String parts[] = locale.split("_", -1);
            Locale l;
            if (parts.length == 1) l = new Locale(parts[0]);
            else if (parts.length == 2
                || (parts.length == 3 && parts[2].startsWith("#")))
                l = new Locale(parts[0], parts[1]);
            else l = new Locale(parts[0], parts[1], parts[2]);
            localeCache.put(locale, l);
        }
        return localeCache.get(locale);
    }
}
