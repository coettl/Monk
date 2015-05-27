package com.linemetrics.monk.processor;

import com.linemetrics.monk.dao.DataItem;
import com.linemetrics.monk.director.RunnerContext;
import org.json.simple.JSONObject;

import java.util.List;

public interface IProcessor {

    public List<DataItem> process(RunnerContext ctx, JSONObject settings, List<DataItem> items)
        throws ProcessorException;

}
