package com.linemetrics.monk.config.dao;

import com.linemetrics.monk.config.ISystemConfigStore;

public class DataStream {

    ISystemConfigStore store;

    int jobId;
    int dataStreamId;

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
            ", dataStreamId=" + dataStreamId +
            '}';
    }
}
