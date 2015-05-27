package com.linemetrics.monk.processor;

import java.util.HashMap;
import java.util.Map;

public class ProcessorFactory {

    private static Map<String, IProcessor> processors = new HashMap<>();

    public static IProcessor getProcessor(String type)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if(processors.containsKey(type)) {
            return processors.get(type);
        }
        IProcessor processor = (IProcessor) Class.forName(type).newInstance();
        processors.put(type, processor);
        return processor;
    }

}
