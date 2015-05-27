package com.linemetrics.monk.config.dao;

import com.linemetrics.monk.config.ISystemConfigStore;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class DataProcessor {

    ISystemConfigStore store;

    int id;
    int jobId;
    String processorType;
    JSONObject properties;

    public DataProcessor(ISystemConfigStore store) {
        this.store = store;
    }

    public int getId() {
        return id;
    }

    public DataProcessor setId(int id) {
        this.id = id;
        return this;
    }

    public int getJobId() {
        return jobId;
    }

    public DataProcessor setJobId(int jobId) {
        this.jobId = jobId;
        return this;
    }

    public String getProcessorType() {
        return processorType;
    }

    public DataProcessor setProcessorType(String processorType) {
        this.processorType = processorType;
        return this;
    }

    public String getProperties() {
        return properties == null ? "{}" : properties.toJSONString();
    }

    public JSONObject getPropertiesObject() {
        return properties;
    }

    public DataProcessor setProperties(String properties) {
        Object json = JSONValue.parse(properties);
        return this.setPropertiesObject(json);
    }

    public DataProcessor setPropertiesObject(Object json) {
        if(json instanceof JSONObject) {
            this.properties = (JSONObject) json;
        } else {
            this.properties = new JSONObject();
        }
        return this;
    }

    @Override
    public String toString() {
        return "DataProcessor{" +
            "id=" + id +
            ", jobId=" + jobId +
            ", processorType='" + processorType + '\'' +
            ", properties=" + properties +
            '}';
    }
}
