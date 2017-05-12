package com.linemetrics.monk.store;

import com.linemetrics.monk.config.dao.DataStream;
import com.linemetrics.monk.dao.DataItem;
import com.linemetrics.monk.director.RunnerContext;
import com.linemetrics.monk.processor.ProcessorException;
import org.json.simple.JSONObject;

import java.util.List;
import java.util.Map;

public interface IStore {

    public boolean initialize(RunnerContext ctx,
                              JSONObject settings,
                              Map<String, String> metaInfos,
                              Map<Integer, Map<String, String>> dataStreamMetaInfos,
                              List<DataStream> ds);

    public boolean store(
            RunnerContext ctx,
            JSONObject settings,
            Map<String, String> metaInfos,
            Map<Integer, Map<String, String>> dataStreamMetaInfos,
            Map<Integer, List<DataItem>> items)
        throws ProcessorException;

}
