package com.linemetrics.monk.config;

import com.linemetrics.monk.config.dao.*;

import java.util.List;
import java.util.Map;

public interface ISystemConfigStore {

    public Api getApiCredentials() throws ConfigException;

    public List<DirectorJob> getDirectorJobs() throws ConfigException;
    public boolean createDirectorJob(DirectorJob job) throws ConfigException;
    public boolean updateDirectorJob(DirectorJob job) throws ConfigException;
    public boolean deleteDirectorJob(int directorJobId) throws ConfigException;

    public List<DataStream> getDataStreams(int directorJobId) throws ConfigException;
    public boolean createDataStream(DataStream dataStream) throws ConfigException;
    public boolean deleteDataStream(DataStream dataStream) throws ConfigException;

    public Map<String, String> getMetaInfo(MetaInfoType metaType, int metaTypeId) throws ConfigException;
    public boolean createMetaInfo(MetaInfoType metaType, int metaTypeId, String metaKey, String metaValue) throws ConfigException;
    public boolean deleteMetaInfoByKey(MetaInfoType metaType, int metaTypeId, String metaKey) throws ConfigException;
    public boolean deleteMetaInfos(MetaInfoType metaType, int metaTypeId) throws ConfigException;

    public List<DataProcessor> getDataProcessors(int directorJobId) throws ConfigException;
    public boolean createDataProcessor(DataProcessor processor) throws ConfigException;
    public boolean updateDataProcessor(DataProcessor processor) throws ConfigException;
    public boolean deleteDataProcessor(int processorId) throws ConfigException;

    public List<DataStore> getDataStores(int directorJobId) throws ConfigException;
    public boolean createDataStore(DataStore processor) throws ConfigException;
    public boolean updateDataStore(DataStore processor) throws ConfigException;
    public boolean deleteDataStore(int dataStoreId) throws ConfigException;


}
