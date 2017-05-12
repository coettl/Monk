package com.linemetrics.monk.processor.plugins.compress;

import com.linemetrics.monk.api.helper.DataItemComparator;
import com.linemetrics.monk.dao.DataItem;
import com.linemetrics.monk.director.RunnerContext;
import com.linemetrics.monk.processor.IProcessor;
import com.linemetrics.monk.processor.ProcessorException;
import org.joda.time.Period;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class CompressorPlugin implements IProcessor {

    enum CompressionMode {
        AVG, SUM
    }

    @Override
    public List<DataItem> process(
            RunnerContext ctx,
            JSONObject settings,
            List<DataItem> items) throws ProcessorException {

        CompressionMode compressionMode =
            settings.containsKey("compression_mode")
                ? CompressionMode.valueOf((String)settings.get("compression_mode"))
                : CompressionMode.AVG;

        String compressionSize =
            settings.containsKey("compression_size")
                ? (String)settings.get("compression_size")
                : "PT15M";

        Integer compressionBatchItemCount =
            settings.containsKey("compression_batch_item_count")
                ? Integer.valueOf((String)settings.get("compression_batch_item_count"))
                : null;

        long compressionSizeMillis = new Period(compressionSize).toStandardDuration().getMillis();

        long startTime = ctx.getTimeFrom();
        long compressionEndTime = ctx.getTimeFrom() + compressionSizeMillis;

        Iterator<DataItem> itemIterator = items.iterator();
        List<DataItem> newList = new ArrayList<>();

        DataItem item;

        Double sum = null, min = null, tmpMin, max = null, tmpMax;
        Long   cnt = null;

        while(itemIterator.hasNext()) {

            item = itemIterator.next();

            if(item.getTimestamp() < ctx.getTimeFrom() ||
                    item.getTimestamp() > ctx.getTimeTo()) {
//                System.out.println("Ignoring " + item);
                continue;
            }

//            System.out.println("Loop " + item);

            if(item.getTimestamp() > compressionEndTime) {

                if(cnt != null && (compressionBatchItemCount == null || compressionBatchItemCount.longValue() == cnt)) {
                    DataItem newItem = createNewItem(startTime,compressionEndTime, min, max, cnt, sum, compressionMode);
                    System.err.println(newItem);
                    newList.add(newItem);
                } else {
                    System.err.format("No valid compression available for %d-%d cnt#%d\n\r",
                        startTime, compressionEndTime,
                        cnt == null ? 0 : cnt);
                }

                sum = min = max = null;
                cnt = null;

                while(item.getTimestamp() > compressionEndTime) {
                    startTime += compressionSizeMillis;
                    compressionEndTime += compressionSizeMillis;
                }
            }

            sum = (sum == null) ? item.getValue() : (sum + item.getValue());
            tmpMin = (item.getMin() == null ? item.getValue() : item.getMin());
            tmpMax = (item.getMax() == null ? item.getValue() : item.getMax());
            min = (min == null) ? tmpMin : (tmpMin < min) ? tmpMin : min;
            max = (max == null) ? tmpMax : (tmpMax > max) ? tmpMax : max;
            cnt = (cnt == null) ? 1 : cnt + 1;
        }

        if(cnt != null && (compressionBatchItemCount == null || compressionBatchItemCount.longValue() == cnt)) {
            DataItem newItem = createNewItem(startTime, compressionEndTime, min, max, cnt, sum, compressionMode);
            System.err.println(newItem);
            newList.add(newItem);
        } else {
            System.err.format("No valid compression available for %d-%d cnt#%d\n\r",
                startTime, compressionEndTime,
                cnt == null ? 0 : cnt);
        }

        Collections.sort(newList, new DataItemComparator());

        return newList;
    }

    private DataItem createNewItem(Long tsStart, Long tsEnd, Double min, Double max, Long cnt, Double sum, CompressionMode cMode) {
        DataItem newItem = new DataItem(null);
        newItem.setTimestampStart(tsStart);
        newItem.setTimestampEnd(tsEnd);
        newItem.setMin(min);
        newItem.setMax(max);
        newItem.setValue((cMode == CompressionMode.AVG && sum != null) ? (sum / cnt) : sum);
        return newItem;
    }
}
