package com.linemetrics.monk.director;

import com.linemetrics.monk.MonkException;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class RunnerContext {

    Integer jobId;
    Long timeFrom;
    Long timeTo;
    Long batchSize;
    String timezone;

    public RunnerContext(
            int jobId,
            long timeFrom, long timeTo,
            long batchSize, String timezone) {

        this.jobId      = jobId;
        this.timeFrom   = timeFrom;
        this.timeTo     = timeTo;
        this.batchSize  = batchSize;
        this.timezone   = timezone;
    }

    public static RunnerContext parse(String json) throws MonkException {
        JSONObject jsonObj = (JSONObject) JSONValue.parse(json);

        if(jsonObj == null) {
            throw new MonkException("Unable to parse Runner Context");
        }

        return new RunnerContext(
            (Integer)jsonObj.get("jobId"),
            (Long)jsonObj.get("timeFrom"),
            (Long)jsonObj.get("timeTo"),
            (Long)jsonObj.get("batchSize"),
            (String)jsonObj.get("timezone")
        );
    }

    public String toJSONString() {
        JSONObject obj=new JSONObject();
        obj.put("jobId", getJobId());
        obj.put("timeFrom", getTimeFrom());
        obj.put("timeTo", getTimeTo());
        obj.put("batchSize", getBatchSize());
        obj.put("timezone", getTimezone());
        return obj.toJSONString();
    }

    public int getJobId() {
        return jobId;
    }

    public long getTimeFrom() {
        return timeFrom;
    }

    public long getTimeTo() {
        return timeTo;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public String getTimezone() {
        return timezone;
    }
}
