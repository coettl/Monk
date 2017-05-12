package com.linemetrics.monk.config.dao;

import com.linemetrics.monk.config.ISystemConfigStore;
import org.json.simple.JSONObject;

public class DataStream {

    ISystemConfigStore store;

    private int jobId;
    private int dataStreamId;

    private JSONObject properties;

    public DataStream(ISystemConfigStore store) {
        this.store = store;
    }

    public int getJobId() {
        return jobId;
    }

    public DataStream setJobId(int jobId) {
        this.jobId = jobId;
        return this;
    }

    public int getDataStreamId() {
        return dataStreamId;
    }

    public DataStream setDataStreamId(int dataStreamId) {
        this.dataStreamId = dataStreamId;
        return this;
    }

    public JSONObject getProperties() {
        if(this.properties == null){
            this.properties = new JSONObject();
        }
        return properties;
    }

    public String getPropertiesAsString(){
        return getProperties().toJSONString();
    }

    public DataStream setProperties(JSONObject properties) {
        this.properties = properties;
        return this;
    }

    public boolean save() {
        try {
            this.store.createDataStream(this);
        } catch(Exception exp) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "DataStream{" +
            "jobId=" + jobId +
            ", id=" + dataStreamId +
            '}';
    }
}
