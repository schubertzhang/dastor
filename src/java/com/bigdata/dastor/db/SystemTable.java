/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bigdata.dastor.db;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.IOError;

import org.apache.log4j.Logger;

import com.bigdata.dastor.config.DatabaseDescriptor;
import com.bigdata.dastor.db.filter.NamesQueryFilter;
import com.bigdata.dastor.db.filter.QueryFilter;
import com.bigdata.dastor.db.filter.QueryPath;
import com.bigdata.dastor.db.marshal.BytesType;
import com.bigdata.dastor.dht.IPartitioner;
import com.bigdata.dastor.dht.Token;
import com.bigdata.dastor.service.StorageService;
import com.bigdata.dastor.utils.FBUtilities;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

public class SystemTable
{
    private static Logger logger = Logger.getLogger(SystemTable.class);
    public static final String STATUS_CF = "LocationInfo"; // keep the old CF string for backwards-compatibility
    private static final String LOCATION_KEY = "L";
    private static final String BOOTSTRAP_KEY = "Bootstrap";
    private static final byte[] BOOTSTRAP = utf8("B");
    private static final byte[] TOKEN = utf8("Token");
    private static final byte[] GENERATION = utf8("Generation");
    private static final byte[] CLUSTERNAME = utf8("ClusterName");
    private static final byte[] PARTITIONER = utf8("Partioner");
    private static StorageMetadata metadata;

    // BIGDATA: for CF status
    public static final String CFSTA_CF = "CFSta";
    private static final String CFSTA_STATUS_KEY = "Status";
    
    private static byte[] utf8(String str)
    {
        try
        {
            return str.getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Record token being used by another node
     */
    public static synchronized void updateToken(InetAddress ep, Token token)
    {
        IPartitioner p = StorageService.getPartitioner();
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(ep.getAddress(), p.getTokenFactory().toByteArray(token), System.currentTimeMillis()));
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
        rm.add(cf);
        try
        {
            rm.apply();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * This method is used to update the System Table with the new token for this node
    */
    public static synchronized void updateToken(Token token)
    {
        assert metadata != null;
        IPartitioner p = StorageService.getPartitioner();
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(SystemTable.TOKEN, p.getTokenFactory().toByteArray(token), System.currentTimeMillis()));
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
        rm.add(cf);
        try
        {
            rm.apply();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        metadata.setToken(token);
    }
    

    /**
     * One of three things will happen if you try to read the system table:
     * 1. files are present and you can read them: great
     * 2. no files are there: great (new node is assumed)
     * 3. files are present but you can't read them: bad (suspect that the partitioner was changed).
     * @throws IOException
     */
    public static void checkHealth() throws IOException
    {
        Table table = null;
        try
        {
            table = Table.open(Table.SYSTEM_TABLE);
        }
        catch (AssertionError err)
        {
            // this happens when a user switches from OPP to RP.
            IOException ex = new IOException("Could not read system table. Did you change partitioners?");
            ex.initCause(err);
            throw ex;
        }
        
        SortedSet<byte[]> cols = new TreeSet<byte[]>(new BytesType());
        cols.add(TOKEN);
        cols.add(GENERATION);
        cols.add(PARTITIONER);
        QueryFilter filter = new NamesQueryFilter(LOCATION_KEY, new QueryPath(STATUS_CF), cols);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);
        
        if (cf == null)
        {
            // this is either a brand new node (there will be no files), or the partitioner was changed from RP to OPP.
            for (String path : DatabaseDescriptor.getAllDataFileLocationsForTable("system"))
            {
                File[] dbContents = new File(path).listFiles(new FilenameFilter()
                {
                    public boolean accept(File dir, String name)
                    {
                        return name.endsWith(".db");
                    }
                }); 
                if (dbContents.length > 0)
                    throw new IOException("Found system table files, but they couldn't be loaded. Did you change the partitioner?");
            }   
            // no system files. data is either in the commit log or this is a new node.
            return;
        }
        
        
        // token and generation should *always* be there. If either are missing, we can assume that the partitioner has
        // been switched.
        if (cf.getColumnCount() > 0 && (cf.getColumn(GENERATION) == null || cf.getColumn(TOKEN) == null))
            throw new IOException("Couldn't read system generation or token. Did you change the partitioner?");
        IColumn partitionerCol = cf.getColumn(PARTITIONER);
        if (partitionerCol != null && !DatabaseDescriptor.getPartitioner().getClass().getName().equals(new String(partitionerCol.value(), "UTF-8")))
            throw new IOException("Detected partitioner mismatch! Did you change the partitioner?");
        if (partitionerCol == null)
            logger.info("Did not see a partitioner in system storage.");
    }    
    
    /*
     * This method reads the system table and retrieves the metadata
     * associated with this storage instance. Currently we store the
     * metadata in a Column Family called LocatioInfo which has two
     * columns namely "Token" and "Generation". This is the token that
     * gets gossiped around and the generation info is used for FD.
     * We also store whether we're in bootstrap mode in a third column
    */
    public static synchronized StorageMetadata initMetadata() throws IOException
    {
        if (metadata != null)  // guard to protect against being called twice
            return metadata;

        /* Read the system table to retrieve the storage ID and the generation */
        Table table = Table.open(Table.SYSTEM_TABLE);
        SortedSet<byte[]> columns = new TreeSet<byte[]>(new BytesType());
        columns.add(TOKEN);
        columns.add(GENERATION);
        columns.add(CLUSTERNAME);
        columns.add(PARTITIONER);
        QueryFilter filter = new NamesQueryFilter(LOCATION_KEY, new QueryPath(STATUS_CF), columns);
        ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);
        String partitioner = DatabaseDescriptor.getPartitioner().getClass().getName();

        IPartitioner p = StorageService.getPartitioner();
        if (cf == null)
        {
            Token token;
            String initialToken = DatabaseDescriptor.getInitialToken();
            if (initialToken == null)
                token = p.getRandomToken();
            else
                token = p.getTokenFactory().fromString(initialToken);

            logger.info("Saved Token not found. Using " + token);
            // seconds-since-epoch isn't a foolproof new generation
            // (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
            // but it's as close as sanely possible
            int generation = (int) (System.currentTimeMillis() / 1000);

            logger.info("Saved ClusterName not found. Using " + DatabaseDescriptor.getClusterName());

            RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
            cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.STATUS_CF);
            cf.addColumn(new Column(TOKEN, p.getTokenFactory().toByteArray(token)));
            cf.addColumn(new Column(GENERATION, FBUtilities.toByteArray(generation)));
            cf.addColumn(new Column(CLUSTERNAME, DatabaseDescriptor.getClusterName().getBytes()));
            cf.addColumn(new Column(PARTITIONER, partitioner.getBytes("UTF-8")));
            rm.add(cf);
            rm.apply();
            try
            {
                table.getColumnFamilyStore(SystemTable.STATUS_CF).forceBlockingFlush();
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            metadata = new StorageMetadata(token, generation, DatabaseDescriptor.getClusterName().getBytes());
            return metadata;
        }

        if (cf.getColumnCount() < 2)
            throw new RuntimeException("Expected both token and generation columns; found " + cf);
        /* we crashed and came back up: make sure new generation is greater than old */
        IColumn tokenColumn = cf.getColumn(TOKEN);
        assert tokenColumn != null : cf;
        Token token = p.getTokenFactory().fromByteArray(tokenColumn.value());
        logger.info("Saved Token found: " + token);

        IColumn generation = cf.getColumn(GENERATION);
        assert generation != null : cf;
        int gen = Math.max(FBUtilities.byteArrayToInt(generation.value()) + 1, (int) (System.currentTimeMillis() / 1000));

        IColumn cluster = cf.getColumn(CLUSTERNAME);
        IColumn partitionerColumn = cf.getColumn(PARTITIONER);

        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, LOCATION_KEY);
        cf = ColumnFamily.create(Table.SYSTEM_TABLE, SystemTable.STATUS_CF);
        Column generation2 = new Column(GENERATION, FBUtilities.toByteArray(gen), generation.timestamp() + 1);
        cf.addColumn(generation2);
        byte[] cname;
        if (cluster != null)
        {
            logger.info("Saved ClusterName found: " + new String(cluster.value()));
            cname = cluster.value();
        }
        else
        {
            Column clustername = new Column(CLUSTERNAME, DatabaseDescriptor.getClusterName().getBytes());
            cf.addColumn(clustername);
            cname = DatabaseDescriptor.getClusterName().getBytes();
            logger.info("Saved ClusterName not found. Using " + DatabaseDescriptor.getClusterName());
        }
                
        if (partitionerColumn == null)
        {
            Column c = new Column(PARTITIONER, partitioner.getBytes("UTF-8"));
            cf.addColumn(c);
            logger.info("Saved partitioner not found. Using " + partitioner);
        }
        
        rm.add(cf);
        rm.apply();
        try
        {
            table.getColumnFamilyStore(SystemTable.STATUS_CF).forceBlockingFlush();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        
        metadata = new StorageMetadata(token, gen, cname);
        return metadata;
    }

    public static boolean isBootstrapped()
    {
        Table table = null;
        try
        {
            table = Table.open(Table.SYSTEM_TABLE);
            QueryFilter filter = new NamesQueryFilter(BOOTSTRAP_KEY, new QueryPath(STATUS_CF), BOOTSTRAP);
            ColumnFamily cf = table.getColumnFamilyStore(STATUS_CF).getColumnFamily(filter);
            return cf != null && cf.getColumn(BOOTSTRAP).value()[0] == 1;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void setBootstrapped(boolean isBootstrapped)
    {
        ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, STATUS_CF);
        cf.addColumn(new Column(BOOTSTRAP, new byte[] { (byte) (isBootstrapped ? 1 : 0) }, System.currentTimeMillis()));
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, BOOTSTRAP_KEY);
        rm.add(cf);
        try
        {
            rm.apply();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    // BIGDATA:
    // Construct the ksName:cfName, which will be the identifier of a target CF.
    // And will be the column name for key/row CF Status in local system table.
    public static String kscfName(String ksName, String cfName)
    {
        // the ksName and cfName cannot include the delimiter ":". 
        return ksName + ":" + cfName;
    }

    // BIGDATA:
    // Split out the ksName from kscfName string.
    public static String ksOfKscfName(String kscfName)
    {
        return kscfName.substring(0, kscfName.indexOf(":"));
    }
    
    // BIGDATA:
    public static String ksOfKscfName(byte[] bKscfName)
    {
        return ksOfKscfName(FBUtilities.utf8String(bKscfName));
    }
    
    // BIGDATA:
    // Split out the cfName from kscfName string.
    public static String cfOfKscfName(String kscfName)
    {
        return kscfName.substring(kscfName.indexOf(":") + 1);
    }
    
    // BIGDATA:
    public static String cfOfKscfName(byte[] bKscfName)
    {
        return cfOfKscfName(FBUtilities.utf8String(bKscfName));
    }
    
    // BIGDATA:
    public static IColumn getCFStatus(String ksName, String cfName)
    {
        byte[] bKscfName = utf8(kscfName(ksName, cfName));
        return getCFStatus(bKscfName);
    }
    
    // BIGDATA:
    public static IColumn getCFStatus(byte[] bKscfName)
    {
        Table table = null;
        try
        {
            table = Table.open(Table.SYSTEM_TABLE);
            QueryFilter filter = new NamesQueryFilter(CFSTA_STATUS_KEY, new QueryPath(CFSTA_CF), bKscfName);
            ColumnFamily cf = table.getColumnFamilyStore(CFSTA_CF).getColumnFamily(filter);            
            if (cf == null)
            {
               return null;
            }
            
            return cf.getColumn(bKscfName);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    // BIGDATA:
    public static void setCFStatus(String ksName, String cfName, int status, long timestamp)
    {
        byte[] bKscfName = utf8(kscfName(ksName, cfName));
        setCFStatus(bKscfName, status, timestamp);
    }
    
    // BIGDATA:
    public static void setCFStatus(byte[] bKscfName, int status, long timestamp)
    {
        try
        {
            byte[] value = FBUtilities.toByteArray(status);
            ColumnFamily cf = ColumnFamily.create(Table.SYSTEM_TABLE, CFSTA_CF);
            cf.addColumn(new Column(bKscfName, value, timestamp));
            RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, CFSTA_STATUS_KEY);
            rm.add(cf);
            rm.apply();
            
            if (logger.isInfoEnabled())
            {
                logger.info("Save CF status to local system table, CF: " 
                        + FBUtilities.utf8String(bKscfName) 
                        + ", status=" + status + ", timestamp=" + timestamp);
            }
        }
        catch (IOException e) 
        {
            logger.error("Save CF status to local system table IOException, CF: " 
                    + FBUtilities.utf8String(bKscfName) 
                    + ", status=" + status + ", timestamp=" + timestamp);
            throw new RuntimeException(e);
        }
    }
    
    // BIGDATA:
    private static void loadKscfMetaFromSystemTable(String keySpace) throws IOException
    {
        for (ColumnFamilyStore cfs : Table.open(keySpace).getColumnFamilyStores())
        {
            IColumn savedStatus = SystemTable.getCFStatus(keySpace, cfs.getColumnFamilyName());
            if (savedStatus != null)
            {
                logger.info("Load saved CFStatus from local system table for CF: "
                        + SystemTable.kscfName(keySpace, cfs.getColumnFamilyName()));
                cfs.setStatus(FBUtilities.byteArrayToInt(savedStatus.value()), 
                        savedStatus.timestamp(), false);
            }
            else
            {
                logger.info("No saved CFStatus in local system table, nOw save it for CF: "
                        + SystemTable.kscfName(keySpace, cfs.getColumnFamilyName()));
                SystemTable.setCFStatus(keySpace, cfs.getColumnFamilyName(), 
                        cfs.getStatus(), cfs.getStatusTimestamp());
            }
        }
    }
    
    /**
     * BIGDATA:
     * Load the dynamic CF metadata (e.g. status) from local system table.
     */
    public static void loadKscfMetaFromSystemTable()
    {        
        try
        {
            Iterator<String> it = DatabaseDescriptor.getTables().iterator();
            while (it.hasNext())
            {
                String keySpace = it.next();
                if (DatabaseDescriptor.isSystemTable(keySpace))
                {
                    continue;
                }
                loadKscfMetaFromSystemTable(keySpace);
            }
            
            // flush system table, release commitlog segment
            Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(SystemTable.CFSTA_CF).forceBlockingFlush();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    public static class StorageMetadata
    {
        private Token token;
        private int generation;
        private byte[] cluster;

        StorageMetadata(Token storageId, int generation, byte[] clustername)
        {
            token = storageId;
            this.generation = generation;
            cluster = clustername;
        }

        public Token getToken()
        {
            return token;
        }

        public void setToken(Token storageId)
        {
            token = storageId;
        }

        public int getGeneration()
        {
            return generation;
        }

        public byte[] getClusterName()
        {
            return cluster;
        }
    }
}
