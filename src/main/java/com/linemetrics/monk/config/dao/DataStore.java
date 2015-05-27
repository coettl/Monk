package com.linemetrics.monk.config.dao;

import com.linemetrics.monk.config.ISystemConfigStore;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class DataStore {

    ISystemConfigStore store;

    int id;
    int jobId;
    String storeType;
    JSONObject properties;

    public DataStore(ISystemConfigStore store) {
        this.store = store;
    }

    public int getId() {
        return id;
    }

    public DataStore setId(int id) {
        this.id = id;
        return this;
    }

    public int getJobId() {
        return jobId;
    }

    public DataStore setJobId(int jobId) {
        this.jobId = jobId;
        return this;
    }

    public String getStoreType() {
        return storeType;
    }

    public DataStore setStoreType(String storeType) {
        this.storeType = storeType;
        return this;
    }

    public String getProperties() {
        return properties == null ? "{}" : properties.toJSONString();
    }

    public JSONObject getPropertiesObject() {
        return properties;
    }

    public DataStore setProperties(String properties) {
        Object json = JSONValue.parse(properties);
        return this.setPropertiesObject(json);
    }

    public DataStore setPropertiesObject(Object json) {
        if(json instanceof JSONObject) {
            this.properties = (JSONObject) json;
        } else {
            this.properties = new JSONObject();
        }
        return this;
    }
}
