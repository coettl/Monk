package com.linemetrics.monk.director;

import com.linemetrics.monk.api.ApiManager;
import com.linemetrics.monk.config.ConfigException;
import com.linemetrics.monk.config.ConfigStoreManager;
import com.linemetrics.monk.config.dao.*;
import com.linemetrics.monk.dao.DataItem;
import com.linemetrics.monk.dao.TDB;
import com.linemetrics.monk.processor.IProcessor;
import com.linemetrics.monk.processor.ProcessorFactory;
import com.linemetrics.monk.store.IStore;
import com.linemetrics.monk.store.StoreFactory;
import org.quartz.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


@DisallowConcurrentExecution
public class DirectorRunner implements Job {

    boolean                             isInitialized = false;

    Map<String, String>                 jobMetaInfo = null;
    Map<Integer, Map<String, String>>   dataStreamMetaInfo = null;
    List<DataStream>                    dataStreams = null;
    List<DataProcessor>                 processors  = null;
    List<DataStore>                     dataStores  = null;
    DirectorJob                         job         = null;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        try {
            JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();

            if(!isInitialized) {
                if( initialize(dataMap)) {
                    isInitialized = true;
                } else {
                    throw new JobExecutionException("Unable to initialize director job");
                }
            }

            RunnerContext ctx = new RunnerContext(
                jobExecutionContext.getScheduledFireTime().getTime() - job.getDurationInMillis(),
                jobExecutionContext.getScheduledFireTime().getTime(),
                job.getBatchSizeInMillis(),
                job.getTimeZone()
            );

            Map<Integer, List<DataItem>> data = new HashMap<>();

            for(DataStream ds : dataStreams) {

                System.out.println(
                    String.format(
                        "Query DataStream %d for TimeRange %d-%d, TimeZone %s, TDB %s",
                        ds.getDataStreamId(),
                        ctx.getTimeFrom(), ctx.getTimeTo(), job.getTimeZone(), TDB.fromMilliseconds(ctx.getBatchSize()).name()));

                data.put(
                    ds.getDataStreamId(),
                    ApiManager.getClient().getRangeOptimized(
                        ds.getDataStreamId(),
                        ctx.getTimeFrom(),
                        ctx.getTimeTo(),
                        TDB.fromMilliseconds(ctx.getBatchSize()),
                        job.getTimeZoneObject()));
            }

//            System.out.println(data);


            Iterator<Map.Entry<Integer, List<DataItem>>> processorIterator = data.entrySet().iterator();
            while(processorIterator.hasNext()) {
                Map.Entry<Integer, List<DataItem>> dataItemData = processorIterator.next();
                List<DataItem> dataItems = dataItemData.getValue();

                for(DataProcessor processor : processors) {
                    IProcessor proc = ProcessorFactory.getProcessor(processor.getProcessorType());
                    List<DataItem> newItems = proc.process(ctx, processor.getPropertiesObject(), dataItems);
                    dataItems = newItems;
                }

                dataItemData.setValue(dataItems);
            }

            for(DataStore store : dataStores) {
                IStore proc = StoreFactory.getStore(store.getStoreType());
                proc.store(ctx, store.getPropertiesObject(), jobMetaInfo, dataStreamMetaInfo, data);
            }

//            System.out.println(jobExecutionContext.getFireTime());
//            System.out.println(dataMap.get("job"));
//            System.out.println(data);

        } catch(Exception exp) {
            exp.printStackTrace();
        }
    }

    private boolean initialize(JobDataMap dataMap) {

        if( ! dataMap.containsKey("job") ||
                ! (dataMap.get("job") instanceof Integer)) {
            return false;
        }

        Integer jobId = (Integer)dataMap.get("job");

        try {
            jobMetaInfo = ConfigStoreManager
                .getSystemStore()
                .getMetaInfo(MetaInfoType.JOB, jobId);

            dataStreams = ConfigStoreManager
                .getSystemStore()
                .getDataStreams(jobId);

            dataStreamMetaInfo = new HashMap<>();
            for(DataStream ds : dataStreams) {
                dataStreamMetaInfo.put(ds.getDataStreamId(), new HashMap<String, String>());
                dataStreamMetaInfo.get(ds.getDataStreamId()).putAll(
                    ConfigStoreManager
                        .getSystemStore()
                        .getMetaInfo(MetaInfoType.DATASTREAM, ds.getDataStreamId())
                );
            }

            processors = ConfigStoreManager
                .getSystemStore()
                .getDataProcessors(jobId);

            dataStores = ConfigStoreManager
                .getSystemStore()
                .getDataStores(jobId);

            job = Director.getDirectorJobById(jobId);
        } catch(ConfigException cexp) {
            /**
             * @todo log exception
             */
            return false;
        }

        return true;
    }
}
