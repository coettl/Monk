package com.linemetrics.monk.store.plugins.csv;

import com.linemetrics.monk.dao.DataItem;
import com.linemetrics.monk.director.RunnerContext;
import com.linemetrics.monk.helper.TemplateParser;
import com.linemetrics.monk.processor.ProcessorException;
import com.linemetrics.monk.store.IStore;
import org.joda.time.Period;
import org.json.simple.JSONObject;

import java.io.*;
import java.util.*;

public class PrefilledPlugin implements IStore {

    @Override
    public boolean store(
            RunnerContext ctx,
            JSONObject settings,
            final Map<String, String> metaInfos,
            final Map<Integer, Map<String, String>> dataStreamMetaInfos,
            Map<Integer, List<DataItem>> items)
        throws ProcessorException {

        String timeScope =
            settings.containsKey("csv_time_scope")
                ? (String)settings.get("csv_time_scope") : "PT1D";

        String timeSlice =
            settings.containsKey("csv_time_slice")
                ? (String)settings.get("csv_time_slice") : "PT1M";

        String numberLocale =
            settings.containsKey("csv_number_locale")
                ? (String)settings.get("csv_number_locale") : "de_AT";

        String headerTemplate =
            settings.containsKey("csv_header_template")
                ? (String)settings.get("csv_header_template") : null;

        String lineTemplate =
            settings.containsKey("csv_line_template")
                ? (String)settings.get("csv_line_template") : null;

        String emptyLineTemplate =
            settings.containsKey("csv_empty_line_template")
                ? (String)settings.get("csv_empty_line_template") : null;

        String fileTemplate =
            settings.containsKey("csv_file_template")
                ? (String)settings.get("csv_file_template") : null;

        String filePath =
            settings.containsKey("csv_file_path")
                ? (String)settings.get("csv_file_path") : "./";

        String lineSeparator =
            settings.containsKey("csv_line_separator")
                ? (String)settings.get("csv_line_separator") : System.lineSeparator();
        lineSeparator = lineSeparator.replaceAll("<CR>", "\r").replaceAll("<LF>", "\n");

        Map<String, FileCache> checkedFiles = new HashMap<>();

        for(final Map.Entry<Integer, List<DataItem>> dataStream : items.entrySet()) {

            Map<String, String> mi = new HashMap<String, String>() {{
                putAll(metaInfos);
                putAll(dataStreamMetaInfos.get(dataStream.getKey()));
            }};

            FileCache fileCache;
            boolean initialCreation = false;

            File exportFile = new File(
                filePath,
                TemplateParser.parse(
                    fileTemplate,
                    numberLocale,
                    mi,
                    ctx,
                    null
                )
            );

            String fileAddress = exportFile.getAbsolutePath();

            if (checkedFiles.containsKey(fileAddress)) {
                fileCache = checkedFiles.get(fileAddress);
            } else {
                fileCache = new FileCache();
                fileCache.fileExists = exportFile.exists();
                fileCache.lineCache = new ArrayList<String>();

                checkedFiles.put(fileAddress, fileCache);

                if(fileCache.fileExists == false) {
                    initialCreation = true;
                }
            }

            if (!fileCache.fileExists && initialCreation) {

                if (headerTemplate != null && initialCreation) {
                    fileCache.lineCache.add(
                        TemplateParser.parse(
                            headerTemplate,
                            numberLocale,
                            mi,
                            ctx,
                            null
                        )
                    );
                }
            }

            if(!fileCache.fileExists) {

                long timeScopeMillis = new Period(timeScope).toStandardDuration().getMillis();
                long timeSliceMillis = new Period(timeSlice).toStandardDuration().getMillis();

                long startTime = ctx.getTimeFrom();
                long timeScopeEndMillis = ctx.getTimeFrom() + timeScopeMillis;

                DataItem di = DataItem.empty();
                for (long ts = startTime; ts < timeScopeEndMillis; ts += timeSliceMillis) {

                    di.setTimestampStart(ts);
                    di.setTimestampEnd(ts + timeSliceMillis);

                    fileCache.lineCache.add(TemplateParser.parse(
                        emptyLineTemplate,
                        numberLocale,
                        mi,
                        ctx,
                        di));
                }
            }

            if(fileCache.fileExists && fileCache.lineCache.isEmpty()) {
                try (BufferedReader br = new BufferedReader(new FileReader(exportFile))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        fileCache.lineCache.add(line);
                    }
                } catch (IOException e) {
                    throw new ProcessorException("Unable to read-in export file: " + e.getMessage());
                }
            }

            String lineKey, newLine;
            int indexOfKey;

            for (DataItem item : dataStream.getValue()) {

                lineKey = TemplateParser.parse(
                    emptyLineTemplate,
                    numberLocale,
                    mi,
                    ctx,
                    item);

                newLine = TemplateParser.parse(
                    lineTemplate,
                    numberLocale,
                    mi,
                    ctx,
                    item);

                indexOfKey = fileCache.lineCache.indexOf(lineKey);

                if (indexOfKey >= 0) {
                    fileCache.lineCache.set(indexOfKey, newLine);
                } else {
                    System.err.println("Unable to find slice of line: " + newLine + ", searching for: " + lineKey);
                }
            }
        }

        for(Map.Entry<String, FileCache> file : checkedFiles.entrySet()) {

            File fold=new File(file.getKey());
            if(fold.exists()) fold.delete();

            try (PrintWriter out =
                     new PrintWriter(
                         new BufferedWriter(
                             new FileWriter(file.getKey(), false)
                         )
                     )) {

                for(String line : file.getValue().lineCache) {
                    out.print(line + lineSeparator);
                }

            } catch (IOException e) {
                throw new ProcessorException("Unable to store data to CSV: " + e.getMessage());
            }
        }

//        System.out.println(TemplateParser.parse(lineTemplate, numberLocale, metaInfos, ctx, null));

        return true;
    }

    public class FileCache {

        public boolean fileExists;
        public List<String> lineCache;

    }
}
