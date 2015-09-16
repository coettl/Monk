package com.linemetrics.monk.store.plugins.csv;

import com.linemetrics.monk.dao.DataItem;
import com.linemetrics.monk.director.RunnerContext;
import com.linemetrics.monk.helper.TemplateParser;
import com.linemetrics.monk.processor.ProcessorException;
import com.linemetrics.monk.store.IStore;
import org.json.simple.JSONObject;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorePlugin implements IStore {

    @Override
    public boolean store(
            RunnerContext ctx,
            JSONObject settings,
            final Map<String, String> metaInfos,
            final Map<Integer, Map<String, String>> dataStreamMetaInfos,
            Map<Integer, List<DataItem>> items)
        throws ProcessorException {

        String numberLocale =
            settings.containsKey("csv_number_locale")
                ? (String)settings.get("csv_number_locale") : "de_AT";

        String headerTemplate =
            settings.containsKey("csv_header_template")
                ? (String)settings.get("csv_header_template") : null;

        String lineTemplate =
            settings.containsKey("csv_line_template")
                ? (String)settings.get("csv_line_template") : null;

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

        for(final Map.Entry<Integer, List<DataItem>> dataStream : items.entrySet()) {

            Map<String, String> mi = new HashMap<String, String>() {{
                putAll(metaInfos);
                putAll(dataStreamMetaInfos.get(dataStream.getKey()));
            }};

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

            boolean append = exportFile.exists();

            try (PrintWriter out =
                     new PrintWriter(
                         new BufferedWriter(
                             new FileWriter(exportFile, true)
                         )
                     )) {

                if (!append && headerTemplate != null) {
                    out.print(
                        TemplateParser.parse(
                            headerTemplate,
                            numberLocale,
                            mi,
                            ctx,
                            null
                        ) + lineSeparator
                    );
                }

//                System.out.println(items);

                for (DataItem item : dataStream.getValue()) {
                    out.print(
                        TemplateParser.parse(
                            lineTemplate,
                            numberLocale,
                            mi,
                            ctx,
                            item) + lineSeparator);
                }

            } catch (IOException e) {
                throw new ProcessorException("Unable to store data to CSV: " + e.getMessage());
            }
        }

//        System.out.println(TemplateParser.parse(lineTemplate, numberLocale, metaInfos, ctx, null));

        return true;
    }
}
