package com.linemetrics.monk.helper;

import com.owlike.genson.Genson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class JsonParser {

    final static Logger logger = LoggerFactory.getLogger(JsonParser.class);

    private static JsonParser instance = null;

    private Genson jsonParser = null;

    public synchronized static JsonParser getInstance() {
        if(instance == null) {
            instance = new JsonParser();
        }
        return instance;
    }

    private JsonParser() {
        jsonParser = new Genson();
    }

    public synchronized String toJson(Map obj) {
        return this.jsonParser.serialize(obj);
    }

    public synchronized String toJson(List obj) {
        return this.jsonParser.serialize(obj);
    }

    public synchronized Map toObject(byte[] byteJson) {
        return toObject(new String(byteJson));
    }

    public synchronized Map toObject(String strJson) {
        Map json = null;
        if(strJson != null) {
            json = jsonParser.deserialize(strJson, Map.class);
        }
        return (Map)json;
    }

    public synchronized List toList(String strJson) {
        List json = null;
        if(strJson != null) {
            json = jsonParser.deserialize(strJson, List.class);
        }
        return (List)json;
    }

}
