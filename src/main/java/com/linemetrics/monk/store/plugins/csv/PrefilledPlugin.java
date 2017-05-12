package com.linemetrics.monk.store.plugins.csv;

import com.linemetrics.monk.config.dao.DataStream;
import com.linemetrics.monk.dao.DataItem;
import com.linemetrics.monk.director.RunnerContext;
import com.linemetrics.monk.helper.TemplateParser;
import com.linemetrics.monk.processor.ProcessorException;
import com.linemetrics.monk.store.IStore;
import org.joda.time.Period;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrefilledPlugin implements IStore {

    private static final Logger logger = LoggerFactory.getLogger(PrefilledPlugin.class);

    @Override
    public boolean initialize(RunnerContext ctx, JSONObject settings, final Map<String, String> metaInfos, final Map<Integer, Map<String, String>> dataStreamMetaInfos, List<DataStream> ds) {
        String numberLocale =
                settings.containsKey("csv_number_locale")
                        ? (String)settings.get("csv_number_locale") : "de_AT";

        String headerTemplate =
                settings.containsKey("csv_header_template")
                        ? (String)settings.get("csv_header_template") : null;

        String emptyLineTemplate =
                settings.containsKey("csv_empty_line_template")
                        ? (String)settings.get("csv_empty_line_template") : null;

        String fileTemplate =
                settings.containsKey("csv_file_template")
                        ? (String)settings.get("csv_file_template") : null;

        String filePath =
                settings.containsKey("csv_file_path")
                        ? (String)settings.get("csv_file_path") : "./";

        String timeScope =
                settings.containsKey("csv_time_scope")
                        ? (String)settings.get("csv_time_scope") : "PT1D";

        String timeSlice =
                settings.containsKey("csv_time_slice")
                        ? (String)settings.get("csv_time_slice") : "PT1M";

        String lineSeparator =
                settings.containsKey("csv_line_separator")
                        ? (String)settings.get("csv_line_separator") : System.lineSeparator();
        lineSeparator = lineSeparator.replaceAll("<CR>", "\r").replaceAll("<LF>", "\n");

        Map<String, FileCache> checkedFiles = new HashMap<>();

        for(final DataStream stream : ds){

            Map<String, String> mi = new HashMap<String, String>() {{
                putAll(metaInfos);
                putAll(dataStreamMetaInfos.get(stream.getDataStreamId()));
            }};

            try {
                FileCache cache = getFileCache(
                        checkedFiles, filePath, headerTemplate, timeScope, timeSlice,
                        fileTemplate, emptyLineTemplate,
                        numberLocale,  mi, ctx, null);

                for(Map.Entry<String, FileCache> file : checkedFiles.entrySet()) {
                    if(!file.getValue().fileExists){
                        try (PrintWriter out =
                                     new PrintWriter(
                                             new BufferedWriter(
                                                     new FileWriter(file.getKey(), false)
                                             )
                                     )) {

                            for(String line : cache.lineCache) {
                                out.print(line + lineSeparator);
                            }

                        } catch (IOException e) {
                            throw new ProcessorException("Unable to store data to CSV: " + e.getMessage());
                        }
                    }
                }
            } catch(Exception e){
                logger.error("Error storing csv file");
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean store(
            RunnerContext ctx,
            JSONObject settings,
            final Map<String, String> metaInfos,
            final Map<Integer, Map<String, String>> dataStreamMetaInfos,
            Map<Integer, List<DataItem>> items)
        throws ProcessorException {

        logger.debug("Write prefilled file with settings: " + settings);
        logger.debug("And context: " + ctx);

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

            String lineKey, newLine;
            int indexOfKey;
            FileCache fileCache;

            for (DataItem item : dataStream.getValue()) {

                fileCache = getFileCache(
                    checkedFiles, filePath, headerTemplate, timeScope, timeSlice,
                    fileTemplate, emptyLineTemplate,
                    numberLocale,  mi, ctx, item);

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

                String id = "[" + indexOfKey + "] ";

                if (indexOfKey >= 0) {
                    fileCache.lineCache.set(indexOfKey, newLine);
                    logger.info(id + "Write line: " + newLine + ", at index: " + lineKey);
                } else {
                    logger.error(id + "Unable to find slice of line: " + newLine + ", searching for: " + lineKey);
                }
            }
        }

        for(Map.Entry<String, FileCache> file : checkedFiles.entrySet()) {
            File fold = new File(file.getKey());
            if(fold.exists()){
                fold.delete();
            }

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

    public FileCache getFileCache(
            Map<String, FileCache> checkedFiles,
            String filePath,
            String headerTemplate,
            String timeScope,
            String timeSlice,
            String fileTemplate,
            String emptyLineTemplate,
            String numberLocale,
            Map<String, String> mi,
            RunnerContext ctx,
            DataItem item) throws ProcessorException {

        FileCache fileCache;

        String fileAddress = TemplateParser.parse(
                                fileTemplate,
                                numberLocale,
                                mi,
                                ctx,
                                null
                            );

        File exportFile = new File(
            filePath, fileAddress
        );

        fileAddress = exportFile.getAbsolutePath();

        if (checkedFiles.containsKey(fileAddress)) {
            return checkedFiles.get(fileAddress);
        }

        fileCache = new FileCache();
        fileCache.fileExists = exportFile.exists();
        fileCache.lineCache = new ArrayList<String>();

        checkedFiles.put(fileAddress, fileCache);

        if( ! fileCache.fileExists  && headerTemplate != null ) {
            fileCache.lineCache.add(
                TemplateParser.parse(
                    headerTemplate,
                    numberLocale,
                    mi,
                    ctx,
                    item
                )
            );
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

        return fileCache;
    }

    public class FileCache {

        public boolean fileExists;
        public List<String> lineCache;

    }
}
