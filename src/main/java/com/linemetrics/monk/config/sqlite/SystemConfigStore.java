package com.linemetrics.monk.config.sqlite;

import com.linemetrics.monk.config.ConfigException;
import com.linemetrics.monk.config.ISystemConfigStore;
import com.linemetrics.monk.config.dao.*;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SystemConfigStore
        extends SqliteConnector
        implements ISystemConfigStore {

    public static final String TABLE_JOBS           = "JOBS";
    public static final String FIELD_JOB_ID         = "JOB_ID";
    public static final String FIELD_JOB_SETTINGS   = "SETTINGS";

    public static final String TABLE_DATASTREAMS        = "DATASTREAMS";
    public static final String FIELD_DS_JOB_ID          = "JOB_ID";
    public static final String FIELD_DS_DATASTREAM_ID   = "DATASTREAM_ID";

    public static final String TABLE_METAINFO       = "METAINFO";
    public static final String FIELD_META_TYPE      = "META_TYPE";
    public static final String FIELD_META_TYPEID    = "META_TYPEID";
    public static final String FIELD_META_KEY       = "META_KEY";
    public static final String FIELD_META_VALUE     = "META_VALUE";

    public static final String TABLE_PROCESSORS     = "PROCESSORS";
    public static final String FIELD_P_ID           = "ID";
    public static final String FIELD_P_JOB_ID       = "JOB_ID";
    public static final String FIELD_P_TYPE         = "PROCESSOR_TYPE";
    public static final String FIELD_P_SETTINGS     = "SETTINGS";

    public static final String TABLE_DATASTORE      = "DATASTORE";
    public static final String FIELD_STORE_ID       = "ID";
    public static final String FIELD_STORE_JOB_ID   = "JOB_ID";
    public static final String FIELD_STORE_TYPE     = "STORE_TYPE";
    public static final String FIELD_STORE_SETTINGS = "SETTINGS";


    public SystemConfigStore(String dbFile) throws Exception {
        super(dbFile);
        checkStore();
    }

    protected void checkStore()
            throws ConfigException {

        try {
            connectDB();

            Statement stmt;
            String sql;

            stmt = getConnection().createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + TABLE_JOBS + " " +
                "(" + FIELD_JOB_ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                " " + FIELD_JOB_SETTINGS + " TEXT)";
            stmt.executeUpdate(sql);
            stmt.close();

            stmt = getConnection().createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + TABLE_DATASTREAMS + " " +
                "(" + FIELD_DS_JOB_ID + " INTEGER," +
                " " + FIELD_DS_DATASTREAM_ID + " INTEGER)";
            stmt.executeUpdate(sql);
            stmt.close();

            stmt = getConnection().createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + TABLE_METAINFO + " " +
                "(" + FIELD_META_TYPE + " VARCHAR(64)," +
                " " + FIELD_META_TYPEID + " INTEGER, " +
                " " + FIELD_META_KEY + " VARCHAR(64), " +
                " " + FIELD_META_VALUE + " VARCHAR(64))";
            stmt.executeUpdate(sql);
            stmt.close();

            stmt = getConnection().createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + TABLE_PROCESSORS + " " +
                "(" + FIELD_P_ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                " " + FIELD_P_JOB_ID + " INTEGER, " +
                " " + FIELD_P_TYPE + " VARCHAR(64), " +
                " " + FIELD_P_SETTINGS + " TEXT)";
            stmt.executeUpdate(sql);
            stmt.close();

            stmt = getConnection().createStatement();
            sql = "CREATE TABLE IF NOT EXISTS " + TABLE_DATASTORE + " " +
                "(" + FIELD_STORE_ID + " INTEGER PRIMARY KEY AUTOINCREMENT," +
                " " + FIELD_STORE_JOB_ID + " INTEGER, " +
                " " + FIELD_STORE_TYPE + " VARCHAR(64), " +
                " " + FIELD_STORE_SETTINGS + " TEXT)";
            stmt.executeUpdate(sql);
            stmt.close();

        } catch(SQLException | SqliteException exp) {
            throw new ConfigException(exp.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    @Override
    public Api getApiCredentials() throws ConfigException {
        return null;
    }

    //region Director Jobs

    public List<DirectorJob> getDirectorJobs()
            throws ConfigException {

        List<DirectorJob> directorJobs = new ArrayList<>();

        try {
            connectDB();

            Statement stmt = getConnection().createStatement();
            ResultSet rs = stmt.executeQuery( "SELECT * FROM " + TABLE_JOBS + ";" );
            while ( rs.next() ) {
                DirectorJob job = new DirectorJob(this);
                job .setId(rs.getInt(FIELD_JOB_ID))
                    .setProperties(rs.getString(FIELD_JOB_SETTINGS));
                directorJobs.add(job);
            }
            rs.close();
            stmt.close();
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }

        return directorJobs;
    }

    public boolean createDirectorJob(DirectorJob job)
        throws ConfigException  {

        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("INSERT INTO " + TABLE_JOBS + " (" +
                " " + FIELD_JOB_SETTINGS + ") VALUES (?);");
            stmt.setString(1, job.getProperties());
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    public boolean updateDirectorJob(DirectorJob job) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("UPDATE " + TABLE_JOBS + " SET " +
                " " + FIELD_JOB_SETTINGS + " = ? WHERE " +
                " " + FIELD_JOB_ID + " = ? LIMIT 1;");
            stmt.setString(1, job.getProperties());
            stmt.setInt(2, job.getId());
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    public boolean deleteDirectorJob(int directorJobId)
        throws ConfigException  {

        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("DELETE FROM " + TABLE_JOBS + " WHERE " +
                " " + FIELD_JOB_ID + " = ?;");
            stmt.setInt(1, directorJobId);
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    //endregion

    //region Data Streams

    public List<DataStream> getDataStreams(int directorJobId) throws ConfigException {

        List<DataStream> dataStreams = new ArrayList<>();

        try {
            connectDB();

            Statement stmt = getConnection().createStatement();
            ResultSet rs = stmt.executeQuery( "SELECT * FROM " + TABLE_DATASTREAMS + ";" );
            while ( rs.next() ) {
                DataStream ds = new DataStream(this);
                ds  .setDataStreamId(rs.getInt(FIELD_DS_DATASTREAM_ID))
                    .setJobId(rs.getInt(FIELD_DS_JOB_ID));
                dataStreams.add(ds);
            }
            rs.close();
            stmt.close();
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }

        return dataStreams;
    }


    public boolean createDataStream(DataStream dataStream) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("INSERT INTO " + TABLE_DATASTREAMS + " (" +
                " " + FIELD_DS_DATASTREAM_ID + ", " +
                " " + FIELD_DS_JOB_ID + ") VALUES (?, ?);");
            stmt.setInt(1, dataStream.getDataStreamId());
            stmt.setInt(2, dataStream.getJobId());
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    public boolean deleteDataStream(DataStream dataStream) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("DELETE FROM " + TABLE_DATASTREAMS + " WHERE " +
                " " + FIELD_DS_DATASTREAM_ID + " = ? AND " +
                " " + FIELD_DS_JOB_ID + " = ? LIMIT 1;");
            stmt.setInt(1, dataStream.getDataStreamId());
            stmt.setInt(2, dataStream.getJobId());
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }
    //endregion

    //region Meta Info

    public Map<String, String> getMetaInfo(MetaInfoType metaType, int metaTypeId) throws ConfigException {

        Map<String, String> metaInfos = new HashMap<>();

        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("SELECT * FROM " + TABLE_METAINFO + " WHERE " +
                " " + FIELD_META_TYPE + " = ? AND " +
                " " + FIELD_META_TYPEID + " = ?;");
            stmt.setString(1, metaType.name());
            stmt.setInt(2, metaTypeId);

            ResultSet rs = stmt.executeQuery();

            while ( rs.next() ) {
                metaInfos.put(rs.getString(FIELD_META_KEY), rs.getString(FIELD_META_VALUE));
            }
            rs.close();
            stmt.close();
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }

        return metaInfos;
    }

    public boolean createMetaInfo(MetaInfoType metaType, int metaTypeId, String metaKey, String metaValue) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("INSERT INTO " + TABLE_METAINFO + " (" +
                " " + FIELD_META_TYPE + ", " +
                " " + FIELD_META_TYPEID + ", " +
                " " + FIELD_META_KEY + ", " +
                " " + FIELD_META_VALUE + ") VALUES (?, ?, ?, ?);");
            stmt.setString(1, metaType.name());
            stmt.setInt(2, metaTypeId);
            stmt.setString(3, metaKey);
            stmt.setString(4, metaValue);
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    public boolean deleteMetaInfoByKey(MetaInfoType metaType, int metaTypeId, String metaKey) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("DELETE FROM " + TABLE_METAINFO + " WHERE " +
                " " + FIELD_META_TYPE + " = ? AND " +
                " " + FIELD_META_TYPEID + " = ? AND " +
                " " + FIELD_META_KEY + " = ? LIMIT 1;");
            stmt.setString(1, metaType.name());
            stmt.setInt(2, metaTypeId);
            stmt.setString(3, metaKey);
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    public boolean deleteMetaInfos(MetaInfoType metaType, int metaTypeId) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("DELETE FROM " + TABLE_METAINFO + " WHERE " +
                " " + FIELD_META_TYPE + " = ? AND " +
                " " + FIELD_META_TYPEID + " = ?;");
            stmt.setString(1, metaType.name());
            stmt.setInt(2, metaTypeId);
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    //endregion

    //region Data Processor

    public List<DataProcessor> getDataProcessors(int directorJobId) throws ConfigException {

        List<DataProcessor> processors = new ArrayList<>();

        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("SELECT * FROM " + TABLE_PROCESSORS + " WHERE " +
                " " + FIELD_P_JOB_ID + " = ?;");
            stmt.setInt(1, directorJobId);

            ResultSet rs = stmt.executeQuery();

            while ( rs.next() ) {
                DataProcessor dp = new DataProcessor(this);
                dp  .setJobId(rs.getInt(FIELD_P_JOB_ID))
                    .setId(rs.getInt(FIELD_P_ID))
                    .setProcessorType(rs.getString(FIELD_P_TYPE))
                    .setProperties(rs.getString(FIELD_P_SETTINGS));
                processors.add(dp);
            }
            rs.close();
            stmt.close();
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }

        return processors;
    }

    public boolean createDataProcessor(DataProcessor processor) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("INSERT INTO " + TABLE_PROCESSORS + " (" +
                " " + FIELD_P_JOB_ID + ", " +
                " " + FIELD_P_TYPE + ", " +
                " " + FIELD_P_SETTINGS + ") VALUES (?, ?, ?);");
            stmt.setInt(1, processor.getJobId());
            stmt.setString(2, processor.getProcessorType());
            stmt.setString(3, processor.getProperties());
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    public boolean updateDataProcessor(DataProcessor processor) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("UPDATE " + TABLE_PROCESSORS + " SET " +
                " " + FIELD_P_SETTINGS + " = ? WHERE " +
                " " + FIELD_P_ID + " = ? LIMIT 1;");
            stmt.setString(1, processor.getProperties());
            stmt.setInt(2, processor.getId());
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    public boolean deleteDataProcessor(int processorId) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("DELETE FROM " + TABLE_PROCESSORS + " WHERE " +
                " " + FIELD_P_ID + " = ? LIMIT 1;");
            stmt.setInt(1, processorId);
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    //endregion

    //region Description
    public List<DataStore> getDataStores(int directorJobId) throws ConfigException {

        List<DataStore> stores = new ArrayList<>();

        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("SELECT * FROM " + TABLE_DATASTORE + " WHERE " +
                " " + FIELD_STORE_JOB_ID + " = ?;");
            stmt.setInt(1, directorJobId);

            ResultSet rs = stmt.executeQuery();

            while ( rs.next() ) {
                DataStore ds = new DataStore(this);
                ds  .setJobId(rs.getInt(FIELD_STORE_JOB_ID))
                    .setId(rs.getInt(FIELD_STORE_ID))
                    .setStoreType(rs.getString(FIELD_STORE_TYPE))
                    .setProperties(rs.getString(FIELD_STORE_SETTINGS));
                stores.add(ds);
            }
            rs.close();
            stmt.close();
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }

        return stores;
    }

    public boolean createDataStore(DataStore store) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("INSERT INTO " + TABLE_DATASTORE + " (" +
                " " + FIELD_STORE_JOB_ID + ", " +
                " " + FIELD_STORE_TYPE + ", " +
                " " + FIELD_STORE_SETTINGS + ") VALUES (?, ?, ?);");
            stmt.setInt(1, store.getJobId());
            stmt.setString(2, store.getStoreType());
            stmt.setString(3, store.getProperties());
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    public boolean updateDataStore(DataStore store) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("UPDATE " + TABLE_DATASTORE + " SET " +
                " " + FIELD_STORE_SETTINGS + " = ? WHERE " +
                " " + FIELD_STORE_ID + " = ? LIMIT 1;");
            stmt.setString(1, store.getProperties());
            stmt.setInt(2, store.getId());
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }

    public boolean deleteDataStore(int dataStoreId) throws ConfigException {
        try {
            connectDB();

            PreparedStatement stmt = getConnection().prepareStatement("DELETE FROM " + TABLE_DATASTORE + " WHERE " +
                " " + FIELD_STORE_ID + " = ? LIMIT 1;");
            stmt.setInt(1, dataStoreId);
            stmt.executeUpdate();

            stmt.close();

            return true;
        } catch(SQLException | SqliteException e) {
            throw new ConfigException(e.getMessage());
        } finally {
            try {
                closeDB();
            } catch(SqliteException exp) {
                throw new ConfigException(exp.getMessage());
            }
        }
    }
    //endregion

}
