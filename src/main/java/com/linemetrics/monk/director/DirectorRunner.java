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

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DirectorRunner {

    public static final int WAIT_FOR_NEW_CONTEXT = 1000;

    public static DirectorRunner instance = null;

    public Queue<RunnerContext> context;
    public ContextWorker worker;

    public static DirectorRunner getInstance() {
        if(instance == null) {
            instance = new DirectorRunner();
        }
        return instance;
    }

    private DirectorRunner() {
        context = new ConcurrentLinkedQueue<>();

        worker  = new ContextWorker();
        worker.start();
    }

    public void addContext(RunnerContext ctx) {
        this.context.add(ctx);
    }

    public class ContextWorker extends Thread {

        private Map<String, String>                 jobMetaInfo = null;
        private Map<Integer, Map<String, String>>   dataStreamMetaInfo = null;
        private List<DataStream>                    dataStreams = null;
        private List<DataProcessor>                 processors  = null;
        private List<DataStore>                     dataStores  = null;
        private DirectorJob                         job         = null;

        @Override
        public void run() {
            super.run();

            while(true) {
                try {
                    RunnerContext ctx = context.poll();
                    if (ctx == null) {
                        Thread.sleep(WAIT_FOR_NEW_CONTEXT);
                        continue;
                    }

                    this.initializeEnvironment(ctx.getJobId());

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
                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        private boolean initializeEnvironment(Integer jobId) {
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

}
