package com.linemetrics.monk.config.file;

import com.linemetrics.monk.config.ConfigException;
import com.linemetrics.monk.config.ISystemConfigStore;
import com.linemetrics.monk.config.dao.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.json.simple.JSONObject;

import java.io.FileNotFoundException;
import java.util.*;

public class SystemConfigStore
    implements ISystemConfigStore {

    private String propertyFile = null;

    private PropertiesConfiguration propertyConfiguration = null;

    public SystemConfigStore(String file) throws FileNotFoundException, ConfigurationException {
        propertyFile = file;
        this.reload();
    }

    public void reload() throws FileNotFoundException, ConfigurationException {
        if(this.propertyConfiguration == null) {
            if(propertyFile == null) {
                throw new FileNotFoundException(
                    "No file given for loading properties");
            }
            this.propertyConfiguration = new PropertiesConfiguration();
            this.propertyConfiguration.setDelimiterParsingDisabled(true);
            this.propertyConfiguration.setListDelimiter('~');
            this.propertyConfiguration.load(propertyFile);
            this.propertyFile = this.propertyConfiguration.getFileName();
        }

        if(this.propertyConfiguration.getFileName() != propertyFile) {
            this.propertyConfiguration.setFileName(propertyFile);
            this.reload();
            return;
        }

        this.propertyConfiguration.reload();
    }

    public static <T> T last(T[] array) {
        return array[array.length - 1];
    }

    @Override
    public Api getApiCredentials() throws ConfigException {
        return new Api(
            this,
            propertyConfiguration.getString("api.endpoint"),
            propertyConfiguration.getString("api.hash")
        );
    }

    @Override
    public List<DirectorJob> getDirectorJobs() throws ConfigException {
        List<DirectorJob> directorJobs = new ArrayList<>();

        String[] jobs = propertyConfiguration.getStringArray("activated_jobs");

        for(int i = 0; i < jobs.length; i++) {
            final String key = jobs[i];

            DirectorJob job = new DirectorJob(this);
            job .setId(Integer.valueOf(key));
            job .setPropertiesObject(new JSONObject(){{
                put("scheduler_mask", propertyConfiguration.getString("job." + key + ".info.scheduler_mask"));
                put("timezone",       propertyConfiguration.getString("job." + key + ".info.timezone"));
                put("batch_size",     propertyConfiguration.getString("job." + key + ".info.batch_size"));
                put("duration",       propertyConfiguration.getString("job." + key + ".info.duration"));
            }});
            directorJobs.add(job);
        }

//        System.exit(-1);

        return directorJobs;
    }

    @Override
    public boolean createDirectorJob(DirectorJob job) throws ConfigException {
        return false;
    }

    @Override
    public boolean updateDirectorJob(DirectorJob job) throws ConfigException {
        return false;
    }

    @Override
    public boolean deleteDirectorJob(int directorJobId) throws ConfigException {
        return false;
    }

    @Override
    public List<DataStream> getDataStreams(int directorJobId) throws ConfigException {
        List<DataStream> dataStreams = new ArrayList<>();

        String[] arrDataStreams = propertyConfiguration.getStringArray("job." + directorJobId + ".datastream");
        for(int i = 0; i < arrDataStreams.length; i++) {
            DataStream ds = new DataStream(this);
            ds  .setDataStreamId(Integer.valueOf(arrDataStreams[i]))
                .setJobId(directorJobId);
            dataStreams.add(ds);
        }

        return dataStreams;
    }

    @Override
    public boolean createDataStream(DataStream dataStream) throws ConfigException {
        return false;
    }

    @Override
    public boolean deleteDataStream(DataStream dataStream) throws ConfigException {
        return false;
    }

    @Override
    public Map<String, String> getMetaInfo(MetaInfoType metaType, int metaTypeId) throws ConfigException {
        Map<String, String> metaInfos = new HashMap<>();

        String basePath = "meta." + metaType.name().toLowerCase() + "." + metaTypeId;

        Iterator<String> it = propertyConfiguration.getKeys(basePath);
        while(it.hasNext()) {
            final String key = it.next().replace(basePath + ".", "");
            metaInfos.put(key, propertyConfiguration.getString(basePath + "." + key));
        }

        return metaInfos;
    }

    @Override
    public boolean createMetaInfo(MetaInfoType metaType, int metaTypeId, String metaKey, String metaValue) throws ConfigException {
        return false;
    }

    @Override
    public boolean deleteMetaInfoByKey(MetaInfoType metaType, int metaTypeId, String metaKey) throws ConfigException {
        return false;
    }

    @Override
    public boolean deleteMetaInfos(MetaInfoType metaType, int metaTypeId) throws ConfigException {
        return false;
    }

    @Override
    public List<DataProcessor> getDataProcessors(int directorJobId) throws ConfigException {
        List<DataProcessor> processors = new ArrayList<>();

        String basePath = "job." + directorJobId + ".processor";
        JSONObject settings = new JSONObject();
        DataProcessor processor = new DataProcessor(this);

        Iterator<String> keys = propertyConfiguration.getKeys(basePath);
        while(keys.hasNext()) {

            String key = keys.next().replace(basePath + ".", "");

            if(key.equals("type")) {
                processor.setProcessorType(propertyConfiguration.getString(basePath + "." + key));
            } else {
                settings.put(key, propertyConfiguration.getString(basePath + "." + key));
            }
        }
        processor.setPropertiesObject(settings);

        if( processor.getProcessorType() != null) {
            processors.add(processor);
        }

//        System.out.println("Processors: " + processors);

        return processors;
    }

    @Override
    public boolean createDataProcessor(DataProcessor processor) throws ConfigException {
        return false;
    }

    @Override
    public boolean updateDataProcessor(DataProcessor processor) throws ConfigException {
        return false;
    }

    @Override
    public boolean deleteDataProcessor(int processorId) throws ConfigException {
        return false;
    }

    @Override
    public List<DataStore> getDataStores(int directorJobId) throws ConfigException {
        List<DataStore> stores = new ArrayList<>();

        String basePath = "job." + directorJobId + ".store";
        JSONObject settings = new JSONObject();
        DataStore store = new DataStore(this);

        Iterator<String> keys = propertyConfiguration.getKeys(basePath);
        while(keys.hasNext()) {

            String key = keys.next().replace(basePath + ".", "");

            if(key.equals("type")) {
                store.setStoreType(propertyConfiguration.getString(basePath + "." + key));
            } else {
                settings.put(key, propertyConfiguration.getString(basePath + "." + key));
            }
        }
        store.setPropertiesObject(settings);

        if( store.getStoreType() != null) {
            stores.add(store);
        }

        return stores;
    }

    @Override
    public boolean createDataStore(DataStore processor) throws ConfigException {
        return false;
    }

    @Override
    public boolean updateDataStore(DataStore processor) throws ConfigException {
        return false;
    }

    @Override
    public boolean deleteDataStore(int dataStoreId) throws ConfigException {
        return false;
    }
}
