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

package com.bigdata.dastor.config;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.log4j.Logger;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.bigdata.dastor.auth.AllowAllAuthenticator;
import com.bigdata.dastor.auth.IAuthenticator;
import com.bigdata.dastor.cfc.Collector;
import com.bigdata.dastor.cfc.IBucketMapper;
import com.bigdata.dastor.db.ColumnFamily;
import com.bigdata.dastor.db.HintedHandOffManager;
import com.bigdata.dastor.db.SystemTable;
import com.bigdata.dastor.db.Table;
import com.bigdata.dastor.db.commitlog.CommitLog;
import com.bigdata.dastor.db.marshal.AbstractType;
import com.bigdata.dastor.db.marshal.BytesType;
import com.bigdata.dastor.db.marshal.UTF8Type;
import com.bigdata.dastor.dht.IPartitioner;
import com.bigdata.dastor.dht.RandomPartitioner;
import com.bigdata.dastor.io.compress.Compression;
import com.bigdata.dastor.io.util.FileUtils;
import com.bigdata.dastor.locator.AbstractReplicationStrategy;
import com.bigdata.dastor.locator.DynamicEndpointSnitch;
import com.bigdata.dastor.locator.EndPointSnitch;
import com.bigdata.dastor.locator.IEndPointSnitch;
import com.bigdata.dastor.locator.RackUnawareStrategy;
import com.bigdata.dastor.utils.FBUtilities;
import com.bigdata.dastor.utils.XMLUtils;

public class DatabaseDescriptor
{
    private static Logger logger = Logger.getLogger(DatabaseDescriptor.class);
    public static final String STREAMING_SUBDIR = "stream";

    // don't capitalize these; we need them to match what's in the config file for CLS.valueOf to parse
    public static enum CommitLogSync {
        periodic,
        batch
    }

    public static enum DiskAccessMode {
        auto,
        mmap,
        mmap_index_only,
        standard,
    }

    public static final String random = "RANDOM";
    public static final String ophf = "OPHF";
    private static int storagePort = 9100; // BIGDATA
    private static int thriftPort = 9110;  // BIGDATA
    private static boolean thriftFramed = false;
    private static InetAddress listenAddress = null; // BIGDATA: leave null so we can fall through to getLocalHost
    private static InetAddress thriftAddress = null; // BIGDATA: leave null so we can fall through to getLocalHost
    private static String clusterName = "Test";
    private static long rpcTimeoutInMillis = 10000; // BIGDATA: from 2000
    private static int phiConvictThreshold = 16; // BIGDATA: from 8
    private static Set<InetAddress> seeds = new HashSet<InetAddress>();
    /* Keeps the list of data file directories */
    private static String[] dataFileDirectories;
    /* Current index into the above list of directories */
    private static int currentIndex = 0;
    private static String logFileDirectory;
    private static String savedCachesDirectory;
    private static int consistencyThreads = 4; // not configurable
    private static int concurrentReaders = 8;
    private static int concurrentWriters = 32;

    private static double flushDataBufferSizeInMB = 32;
    private static double flushIndexBufferSizeInMB = 8;
    private static int slicedReadBufferSizeInKB = 64;

    static Map<String, KSMetaData> tables = new HashMap<String, KSMetaData>();
    private static int bmtThreshold = 256;
    /* if this a row exceeds this threshold, we issue warnings during compaction */
    private static long rowWarningThreshold = 64 * 1024 * 1024;

    /* Hashing strategy Random or OPHF */
    private static IPartitioner partitioner = new RandomPartitioner(); // BIGDATA

    /* if the size of columns or super-columns are more than this, indexing will kick in */
    private static int columnIndexSizeInKB = 64; // BIGDATA
    /* Number of minutes to keep a memtable in memory */
    private static int memtableLifetimeMs = 60 * 60 * 1000;
    /* Size of the memtable in memory before it is dumped */
    private static int memtableThroughput = 64;
    /* Number of objects in millions in the memtable before it is dumped */
    private static double memtableOperations = 100; // BIGDATA: large enough to make ineffective.
    /* 
     * This parameter enables or disables consistency checks. 
     * If set to false the read repairs are disable for very
     * high throughput on reads but at the cost of consistency.
    */
    private static boolean doConsistencyCheck = true;
    /* Job Jar Location */
    private static String jobJarFileLocation;
    /* Address where to run the job tracker */
    private static String jobTrackerHost;    
    /* time to wait before garbage collecting tombstones (deletion markers) */
    private static int gcGraceInSeconds = 10 * 24 * 3600; // 10 days

    // the path qualified config file (system-conf.xml) name
    private static String configFileName;
    /* initial token in the ring */
    private static String initialToken = null;

    private static CommitLogSync commitLogSync = CommitLogSync.periodic; // BIGDATA: initialize
    private static double commitLogSyncBatchMS = 0; // BIGDATA: disable, each write will be synced to disk individually
    private static int commitLogSyncPeriodMS = 10000; // BIGDATA: initialize

    // BIGDATA: initialize
    private static DiskAccessMode diskAccessMode = DiskAccessMode.standard;  // BIGDATA: initialize
    private static DiskAccessMode indexAccessMode = DiskAccessMode.standard; // BIGDATA: initialize

    private static boolean snapshotBeforeCompaction = false; // BIGDATA: initialize
    private static boolean autoBootstrap = false;

    private static boolean hintedHandoffEnabled = true;

    private static IAuthenticator authenticator = new AllowAllAuthenticator();

    private static int indexinterval = 128;

    private final static String SYSTEM_CONF_FILE = "system-conf.xml";

    // BIGDATA: the optional schema config file.
    private final static String SCHEMA_CONF_FILE = "schema-conf.xml";

    // BIGDATA: for new row format, is the index at end of row?
    // the first byte of a row is row format.
    private static boolean newRowFormatIndexAtEnd = false;
    
    // BIGDATA: only when a row larger than this size, compression will be applied.
    private static int compressStartRowSize = 0;

    // BIGDATA: try to lock memory.
    private static boolean tryLockMemoryEnabled = true;
    
    // BIGDATA: 
    // the time-segment within a day to run CFC task periodically.
    // the format pattern should be "HH:mm" or "HH:mm:ss".
    private static String cfcBeginTime = null;
    private static String cfcEndTime = null;

    // BIGDATA:
    // concurrent compaction of different CFs
    private static boolean concurrentCompactionEnabled = false;

    public static final int DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS = 0;
    public static final int DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS = 0;

    public static File getSerializedRowCachePath(String ksName, String cfName)
    {
        return new File(savedCachesDirectory + File.separator + ksName + "-" + cfName + "-RowCache");
    }

    public static File getSerializedKeyCachePath(String ksName, String cfName)
    {
        return new File(savedCachesDirectory + File.separator + ksName + "-" + cfName + "-KeyCache");
    }

    public static int getCompactionPriority()
    {
        String priorityString = System.getProperty("bigdata.dastor.compaction.priority");
        return priorityString == null ? Thread.NORM_PRIORITY : Integer.parseInt(priorityString);
    }

    
    /**
     * Try the bigdata.conf.dir system property, and then inspect the classpath.
     */
    static String getSystemConfigPath()
    {
        String scp = System.getProperty("bigdata.conf.dir") + File.separator + SYSTEM_CONF_FILE;
        if (new File(scp).exists())
            return scp;
        // try the classpath
        ClassLoader loader = DatabaseDescriptor.class.getClassLoader();
        URL scpurl = loader.getResource(SYSTEM_CONF_FILE);
        if (scpurl != null)
            return scpurl.getFile();
        throw new RuntimeException("No found system config file: " + SYSTEM_CONF_FILE);
    }
    
    /**
     * BIGDATA:
     * Try the bigdata.conf.dir system property, and then inspect the classpath.
     */
    static String getSchemaConfigPath()
    {
        String scp = System.getProperty("bigdata.conf.dir") + File.separator + SCHEMA_CONF_FILE;
        if (new File(scp).exists())
            return scp;
        // try the classpath
        ClassLoader loader = DatabaseDescriptor.class.getClassLoader();
        URL scpurl = loader.getResource(SCHEMA_CONF_FILE);
        if (scpurl != null)
            return scpurl.getFile();
        
        logger.info("No found schema config file: " + SCHEMA_CONF_FILE);
        return null; // this file is optional file.
    }

    static
    {
        try
        {
            configFileName = getSystemConfigPath();
            if (logger.isDebugEnabled())
                logger.debug("Loading settings from " + configFileName);
            XMLUtils xmlUtils = new XMLUtils(configFileName);

            /* Cluster Name */
            clusterName = xmlUtils.getNodeValue("/Dastor/ClusterName");
            if (clusterName == null)
            {
                throw new ConfigurationException("ClusterName is mandatory.");
            }

            String syncRaw = xmlUtils.getNodeValue("/Dastor/LogSyncMode");
            if (syncRaw != null)
            {
                try
                {
                    commitLogSync = CommitLogSync.valueOf(syncRaw);
                }
                catch (IllegalArgumentException e)
                {
                    throw new ConfigurationException("LogSyncMode must be 'periodic' or 'batch'.");
                }
            }
            if (commitLogSync == null)
            {
                throw new ConfigurationException("Bad config LogSyncMode");
            }
            else if (commitLogSync == CommitLogSync.batch)
            {
                String syncBatchMs = xmlUtils.getNodeValue("/Dastor/LogSyncBatchInMS");
                if (syncBatchMs != null)
                {
                    try
                    {
                        commitLogSyncBatchMS = Double.valueOf(syncBatchMs);
                    }
                    catch (Exception e)
                    {
                        throw new ConfigurationException("LogSyncBatchInMS bad value.");
                    }
                }
                logger.debug("LogSyncMode = batch, interval(ms) = " + commitLogSyncBatchMS);
            }
            else
            {
                assert commitLogSync == CommitLogSync.periodic;
                String syncPeriodMs = xmlUtils.getNodeValue("/Dastor/LogSyncPeriodInMS");
                if (syncPeriodMs != null)
                {
                    try
                    {
                        commitLogSyncPeriodMS = Integer.valueOf(syncPeriodMs);
                    }
                    catch (Exception e)
                    {
                        throw new ConfigurationException("LogSyncPeriodInMS bad value.");
                    }
                }
                logger.debug("LogSyncMode = periodic, period(ms) = " + commitLogSyncPeriodMS);
            }

            String modeRaw = xmlUtils.getNodeValue("/Dastor/DiskAccessMode");
            if (modeRaw != null)
            {
                try
                {
                    diskAccessMode = DiskAccessMode.valueOf(modeRaw);
                }
                catch (IllegalArgumentException e)
                {
                    throw new ConfigurationException("DiskAccessMode must be either 'auto', 'mmap', 'mmap_index_only', or 'standard'");
                }
            }
            /* evaluate the DiskAccessMode conf directive, which also affects indexAccessMode selection */
            if (diskAccessMode == DiskAccessMode.auto)
            {
                diskAccessMode = System.getProperty("os.arch").contains("64") ? DiskAccessMode.mmap : DiskAccessMode.standard;
                indexAccessMode = diskAccessMode;
                logger.info("DiskAccessMode 'auto' determined to be " + diskAccessMode + ", indexAccessMode is " + indexAccessMode );
            }
            else if (diskAccessMode == DiskAccessMode.mmap_index_only)
            {
                diskAccessMode = DiskAccessMode.standard;
                indexAccessMode = DiskAccessMode.mmap;
                logger.info("DiskAccessMode is " + diskAccessMode + ", indexAccessMode is " + indexAccessMode );
            }
            else
            {
                indexAccessMode = diskAccessMode;
                logger.info("DiskAccessMode is " + diskAccessMode + ", indexAccessMode is " + indexAccessMode );
            }

            /* Authentication and authorization backend, implementing IAuthenticator */
            String authenticatorClassName = xmlUtils.getNodeValue("/Dastor/Authenticator");
            if (authenticatorClassName != null)
            {
                try
                {
                    Class cls = Class.forName(authenticatorClassName);
                    authenticator = (IAuthenticator) cls.getConstructor().newInstance();
                }
                catch (ClassNotFoundException e)
                {
                    throw new ConfigurationException("Invalid authenticator class " + authenticatorClassName);
                }
            }
            
            /* Hashing strategy */
            String partitionerClassName = xmlUtils.getNodeValue("/Dastor/DHTPartitioner");
            if (partitionerClassName != null)
            {
                try
                {
                    partitioner = FBUtilities.newPartitioner(partitionerClassName);
                }
                catch (Exception e)
                {
                    throw new ConfigurationException("Invalid DHTPartitioner class : " + partitionerClassName);
                }
            }

            /* JobTracker address */
            jobTrackerHost = xmlUtils.getNodeValue("/Dastor/JobTrackerHost");

            /* Job Jar file location */
            jobJarFileLocation = xmlUtils.getNodeValue("/Dastor/JobJarFileLocation");

            String gcGrace = xmlUtils.getNodeValue("/Dastor/GCDelayInSec");
            if ( gcGrace != null )
                gcGraceInSeconds = Integer.parseInt(gcGrace);

            initialToken = xmlUtils.getNodeValue("/Dastor/InitBootPositionCode");
            
            // BIGDATA: to make the InitBootPositionCode configuration of BigInteger simple
            if ((initialToken != null) && (partitioner instanceof RandomPartitioner))
            {
                if (initialToken.contains(":"))
                {
                    String[] tokenParams = initialToken.split(":");
                    if (tokenParams.length != 2)
                    {
                        throw new ConfigurationException("InitBootPositionCode format error!");
                    }
                    initialToken = new BigInteger("170141183460469231731687303715884105728").
                                        divide(new BigInteger(tokenParams[1].trim())).
                                            multiply(new BigInteger(tokenParams[0].trim())).
                                                toString();
                }
            }
            
            /* RPC Timeout */
            String rpcTimeout = xmlUtils.getNodeValue("/Dastor/RpcTimeoutInMs");
            if ( rpcTimeout != null )
                rpcTimeoutInMillis = Integer.parseInt(rpcTimeout);

            /* phi convict threshold for FailureDetector */
            String phiThreshold = xmlUtils.getNodeValue("/Dastor/FDThreshold");
            if ( phiThreshold != null )
                    phiConvictThreshold = Integer.parseInt(phiThreshold);

            if (phiConvictThreshold < 5 || phiConvictThreshold > 16)
            {
                throw new ConfigurationException("FDThreshold must be between 5 and 16");
            }
            
            /* Thread per pool */
            String rawReaders = xmlUtils.getNodeValue("/Dastor/ConcurrentReaders");
            if (rawReaders != null)
            {
                concurrentReaders = Integer.parseInt(rawReaders);
            }
            if (concurrentReaders < 2)
            {
                throw new ConfigurationException("ConcurrentReaders must be at least 2");
            }

            String rawWriters = xmlUtils.getNodeValue("/Dastor/ConcurrentWriters");
            if (rawWriters != null)
            {
                concurrentWriters = Integer.parseInt(rawWriters);
            }
            if (concurrentWriters < 2)
            {
                throw new ConfigurationException("ConcurrentWriters must be at least 2");
            }

            String rawFlushData = xmlUtils.getNodeValue("/Dastor/DataFlushBufferSizeInMB");
            if (rawFlushData != null)
            {
                flushDataBufferSizeInMB = Double.parseDouble(rawFlushData);
            }
            String rawFlushIndex = xmlUtils.getNodeValue("/Dastor/IndexFlushBufferSizeInMB");
            if (rawFlushIndex != null)
            {
                flushIndexBufferSizeInMB = Double.parseDouble(rawFlushIndex);
            }

            String rawSlicedBuffer = xmlUtils.getNodeValue("/Dastor/SeqReadBufferSizeInKB");
            if (rawSlicedBuffer != null)
            {
                slicedReadBufferSizeInKB = Integer.parseInt(rawSlicedBuffer);
            }

            String bmtThresh = xmlUtils.getNodeValue("/Dastor/BulkMemtableSizeInMB");
            if (bmtThresh != null)
            {
                bmtThreshold = Integer.parseInt(bmtThresh);
            }

            /* TCP port on which the storage system listens */
            String port = xmlUtils.getNodeValue("/Dastor/InterComPort");
            if ( port != null )
                storagePort = Integer.parseInt(port);

            /* Local IP or hostname to bind services to */
            String listenAddr = xmlUtils.getNodeValue("/Dastor/InterComAddress");
            if (listenAddr != null)
            {
                if (listenAddr.equals("0.0.0.0"))
                    throw new ConfigurationException("InterComAddress must be a specified interface.");
                try
                {
                    listenAddress = InetAddress.getByName(listenAddr);
                }
                catch (UnknownHostException e)
                {
                    throw new ConfigurationException("Unknown InterComAddress '" + listenAddr + "'");
                }
            }

            /* Local IP or hostname to bind thrift server to */
            String thriftAddr = xmlUtils.getNodeValue("/Dastor/ServerAddress");
            if ( thriftAddr != null )
                thriftAddress = InetAddress.getByName(thriftAddr);

            /* get the thrift port from conf file */
            port = xmlUtils.getNodeValue("/Dastor/ServerPort");
            if (port != null)
                thriftPort = Integer.parseInt(port);

            /* Framed (Thrift) transport (default to "no") */
            String framedRaw = xmlUtils.getNodeValue("/Dastor/ThriftFramedTransport");
            if (framedRaw != null)
            {
                if (framedRaw.equalsIgnoreCase("true") || framedRaw.equalsIgnoreCase("false"))
                {
                    thriftFramed = Boolean.valueOf(framedRaw);
                }
                else
                {
                    throw new ConfigurationException("Unrecognized value for ThriftFramedTransport.  Use 'true' or 'false'.");
                }
            }

            /* snapshot-before-compaction.  defaults to false */
            String sbc = xmlUtils.getNodeValue("/Dastor/SnapshotBeforeCompaction");
            if (sbc != null)
            {
                if (sbc.equalsIgnoreCase("true") || sbc.equalsIgnoreCase("false"))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("setting snapshotBeforeCompaction to " + sbc);
                    snapshotBeforeCompaction = Boolean.valueOf(sbc);
                }
                else
                {
                    throw new ConfigurationException("Unrecognized value for SnapshotBeforeCompaction.  Use 'true' or 'false'.");
                }
            }

            /* snapshot-before-compaction.  defaults to false */
            String autoBootstr = xmlUtils.getNodeValue("/Dastor/BootupMode");
            if (autoBootstr != null)
            {
                if (logger.isDebugEnabled())
                    logger.debug("setting BootupMode to " + autoBootstr);
                if (autoBootstr.equalsIgnoreCase("self"))
                {
                    autoBootstrap = false;
                }
                else if (autoBootstr.equalsIgnoreCase("collaborative"))
                {
                    autoBootstrap = true;
                }
                else
                {
                    throw new ConfigurationException("BootupMode must be self or collaborative.");
                }
            }

            /* Number of days to keep the memtable around w/o flushing */
            String lifetime = xmlUtils.getNodeValue("/Dastor/MemtableTTLInMinute");
            if (lifetime != null)
                memtableLifetimeMs = Integer.parseInt(lifetime) * 60 * 1000;

            /* Size of the memtable in memory in MB before it is dumped */
            String memtableSize = xmlUtils.getNodeValue("/Dastor/MemtableSizeInMB");
            if ( memtableSize != null )
                memtableThroughput = Integer.parseInt(memtableSize);
            /* Number of objects in millions in the memtable before it is dumped */
            String memtableObjectCount = xmlUtils.getNodeValue("/Dastor/MemtableCellsInMillion");
            if ( memtableObjectCount != null )
                memtableOperations = Double.parseDouble(memtableObjectCount);
            if (memtableOperations <= 0)
            {
                throw new ConfigurationException("Memtable cells count must be a positive double");
            }

            /* This parameter enables or disables consistency checks.
             * If set to false the read repairs are disable for very
             * high throughput on reads but at the cost of consistency.*/
            String doConsistency = xmlUtils.getNodeValue("/Dastor/ConsistencyCheckEnabled");
            if ( doConsistency != null )
                doConsistencyCheck = Boolean.parseBoolean(doConsistency);

            /* read the size at which we should do column indexes */
            String columnIndexSize = xmlUtils.getNodeValue("/Dastor/CellIndexBlockSizeInKB");
            if(columnIndexSize == null)
            {
                columnIndexSizeInKB = 64;
            }
            else
            {
                columnIndexSizeInKB = Integer.parseInt(columnIndexSize);
            }

            String rowWarning = xmlUtils.getNodeValue("/Dastor/RowSizeWarningInMB");
            if (rowWarning != null)
            {
                rowWarningThreshold = Long.parseLong(rowWarning) * 1024 * 1024;
                if (rowWarningThreshold <= 0)
                    throw new ConfigurationException("Row warning line must be a positive integer");
            }
            /* data file and commit log directories. they get created later, when they're needed. */
            dataFileDirectories = xmlUtils.getNodeValues("/Dastor/DataLocations/Location");
            logFileDirectory = xmlUtils.getNodeValue("/Dastor/LogLocation");
            savedCachesDirectory = xmlUtils.getNodeValue("/Dastor/CacheLocation");
            if ((dataFileDirectories == null) || (logFileDirectory == null) || (savedCachesDirectory == null))
            {
                throw new ConfigurationException("DataLocation,LogLocation and CacheLocation are required.");
            }
            for (String datadir : dataFileDirectories)
            {
                if (datadir.equals(logFileDirectory))
                    throw new ConfigurationException("LogLocation and DataLocations must be different.");
                if (datadir.equals(savedCachesDirectory))
                    throw new ConfigurationException("CacheLocation and DataLocations must be different.");
            }

            /* threshold after which commit log should be rotated. */
            String value = xmlUtils.getNodeValue("/Dastor/LogSegmentSizeInMB");
            if ( value != null)
                CommitLog.setSegmentSize(Integer.parseInt(value) * 1024 * 1024);

            /* should Hinted Handoff be on? */
            String hintedHandOffStr = xmlUtils.getNodeValue("/Dastor/DeputyTransferEnabled");
            if (hintedHandOffStr != null)
            {
                if (hintedHandOffStr.equalsIgnoreCase("true"))
                    hintedHandoffEnabled = true;
                else if (hintedHandOffStr.equalsIgnoreCase("false"))
                    hintedHandoffEnabled = false;
                else
                    throw new ConfigurationException("DeputyTransferEnabled bad value. Use 'true' or 'false'.");
            }
            if (logger.isDebugEnabled())
                logger.debug("Setting DeputyTransferEnabled to " + hintedHandoffEnabled);

            String indexIntervalStr = xmlUtils.getNodeValue("/Dastor/IndexInterval");
            if (indexIntervalStr != null)
            {
                indexinterval = Integer.parseInt(indexIntervalStr);
                if (indexinterval <= 0)
                    throw new ConfigurationException("Index Interval must be a positive, non-zero integer.");
            }
            
            // BIGDATA
            String newRowFormatIndexAtEndStr = xmlUtils.getNodeValue("/Dastor/NewRowFormatIndexAtEnd");
            if (newRowFormatIndexAtEndStr != null)
            {
                if (newRowFormatIndexAtEndStr.equalsIgnoreCase("true"))
                    newRowFormatIndexAtEnd = true;
                else if (newRowFormatIndexAtEndStr.equalsIgnoreCase("false"))
                    newRowFormatIndexAtEnd = false;
                else
                    throw new ConfigurationException("NewRowFormatIndexAtEnd bad value. Use 'true' or 'false'.");
            }

            // BIGDATA
            String compressStartRowSizeStr = xmlUtils.getNodeValue("/Dastor/CompressStartRowSize");
            if (compressStartRowSizeStr != null)
            {
                compressStartRowSize = Integer.parseInt(compressStartRowSizeStr);
            }
            
            // BIGDATA
            String tryLockMemoryStr = xmlUtils.getNodeValue("/Dastor/TryLockMemory");
            if (tryLockMemoryStr != null)
            {
                if (tryLockMemoryStr.equalsIgnoreCase("true"))
                    tryLockMemoryEnabled = true;
                else if (tryLockMemoryStr.equalsIgnoreCase("false"))
                    tryLockMemoryEnabled = false;
                else
                    throw new ConfigurationException("TryLockMemory bad value. Use 'true' or 'false'.");
            }            

            readTablesFromXml(configFileName);

            // BIGDATA: read the schema config file.
            readTablesFromXml(getSchemaConfigPath());

            if (tables.isEmpty())
                throw new ConfigurationException("No spaces configured");
            
            // Hardcoded system tables
            KSMetaData systemMeta = new KSMetaData(Table.SYSTEM_TABLE, null, -1, null, null);
            tables.put(Table.SYSTEM_TABLE, systemMeta);
            systemMeta.cfMetaData.put(SystemTable.STATUS_CF, new CFMetaData(Table.SYSTEM_TABLE,
                                                                            SystemTable.STATUS_CF,
                                                                            "Standard",
                                                                            new BytesType(),
                                                                            null,
                                                                            "persistent metadata for the local node",
                                                                            0.0,
                                                                            0.01,
                                                                            DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS,
                                                                            DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS,
                                                                            0L,
                                                                            null));

            // BIGDATA: schema changed
            systemMeta.cfMetaData.put(HintedHandOffManager.HINTS_CF, new CFMetaData(Table.SYSTEM_TABLE,
                                                                                    HintedHandOffManager.HINTS_CF,
                                                                                    "Standard",
                                                                                    new BytesType(),
                                                                                    null,
                                                                                    "temp deputy transfer data",
                                                                                    0.0,
                                                                                    0.01,
                                                                                    DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS,
                                                                                    DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS,
                                                                                    0L,
                                                                                    null));

            // BIGDATA: for CF status
            systemMeta.cfMetaData.put(SystemTable.CFSTA_CF, new CFMetaData(Table.SYSTEM_TABLE,
                                                                           SystemTable.CFSTA_CF,
                                                                           "Standard",
                                                                           new UTF8Type(),
                                                                           null,
                                                                           "persistent CF metadata for the local node",
                                                                           0,
                                                                           0.01,
                                                                           DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS,
                                                                           DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS,
                                                                           0L,
                                                                           null));
            
            // BIGDATA:  Load the time-segment within a day to run CFC task periodically.
            cfcBeginTime = xmlUtils.getNodeValue("/Dastor/BucketCollector/BeginTime");
            cfcEndTime = xmlUtils.getNodeValue("/Dastor/BucketCollector/EndTime");
            if ((cfcBeginTime != null) && !Collector.checkTimeFormat(cfcBeginTime))
            {
                throw new ConfigurationException("Invalid BucketCollector/BeginTime.");
            }
            if ((cfcEndTime != null) && !Collector.checkTimeFormat(cfcEndTime))
            {
                throw new ConfigurationException("Invalid BucketCollector/EndTime.");
            }
            
            // BIGDATA: ConcurrentCompaction on different CFs
            String rawConcCompact = xmlUtils.getNodeValue("/Dastor/ConcurrentCompaction");
            if (rawConcCompact != null)
            {
                if (rawConcCompact.equalsIgnoreCase("true") || rawConcCompact.equalsIgnoreCase("false"))
                {
                    concurrentCompactionEnabled = Boolean.valueOf(rawConcCompact);
                }
                else
                {
                    throw new ConfigurationException("Unrecognized value for ConcurrentCompaction.  Use 'true' or 'false'.");
                }
            }
            
            
            /* Load the seeds for node contact points */
            String[] seedsxml = xmlUtils.getNodeValues("/Dastor/Seeds/Seed");
            if (seedsxml.length <= 0)
            {
                throw new ConfigurationException("At least one seed is required.");
            }
            for (String seedString : seedsxml)
            {
                seeds.add(InetAddress.getByName(seedString));
            }
        }
        catch (UnknownHostException e)
        {
            logger.error("Fatal error: " + e.getMessage());
            System.err.println("Unknown hosts configured. Please use IP addresses instead of hostnames.");
            System.exit(2);
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal error: " + e.getMessage());
            System.err.println("Bad configuration; failure to start dastor service.");
            System.exit(1);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void readTablesFromXml(String xmlFileName) throws ConfigurationException
    {
        if (xmlFileName == null)
        {
            return;
        }
        
        XMLUtils xmlUtils = null;
        try
        {
            xmlUtils = new XMLUtils(xmlFileName);
        }
        catch (ParserConfigurationException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (SAXException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }

            /* Read the table related stuff from config */
        try
        {
            NodeList tablesxml = xmlUtils.getRequestedNodeList("/Dastor/Spaces/Space");
            int size = tablesxml.getLength();
            for ( int i = 0; i < size; ++i )
            {
                String value = null;
                Node table = tablesxml.item(i);

                /* parsing out the table ksName */
                String ksName = XMLUtils.getAttributeValue(table, "Name");
                if (ksName == null)
                {
                    throw new ConfigurationException("Space Name attribute is required");
                }
                if (ksName.equalsIgnoreCase(Table.SYSTEM_TABLE))
                {
                    throw new ConfigurationException("'system' is a reserved table name for Dastor internals");
                }

                /* See which replica placement strategy to use */
                String replicaPlacementStrategyClassName = xmlUtils.getNodeValue("/Dastor/Spaces/Space[@Name='" + ksName + "']/ReplicationPolicy");
                Class<? extends AbstractReplicationStrategy> repStratClass = RackUnawareStrategy.class;;
                if (replicaPlacementStrategyClassName == null)
                {
                    // throw new ConfigurationException("Missing ReplicationPolicy directive for " + ksName);
                    logger.debug("Use default ReplicationPolicy: " + repStratClass.getName());
                }
                else
                {
                    try
                    {
                        repStratClass = (Class<? extends AbstractReplicationStrategy>) Class.forName(replicaPlacementStrategyClassName);
                    }
                    catch (ClassNotFoundException e)
                    {
                        throw new ConfigurationException("Invalid ReplicationPolicy " + replicaPlacementStrategyClassName);
                    }
                }

                /* Data replication factor */
                String replicationFactor = xmlUtils.getNodeValue("/Dastor/Spaces/Space[@Name='" + ksName + "']/ReplicationFactor");
                int repFact = -1;
                if (replicationFactor == null)
                    throw new ConfigurationException("Missing ReplicationFactor for space " + ksName);
                else
                {
                    repFact = Integer.parseInt(replicationFactor);
                }

                /* end point snitch */
                String endPointSnitchClassName = xmlUtils.getNodeValue("/Dastor/Spaces/Space[@Name='" + ksName + "']/NodeTopoLocator");
                IEndPointSnitch epSnitch = new EndPointSnitch();
                if (endPointSnitchClassName == null)
                {
                    // throw new ConfigurationException("Missing NodeTopoLocator directive for space " + ksName);
                    logger.debug("Use default NodeTopoLocator: " + epSnitch.getClass().getName());
                }
                else
                {
                    try
                    {
                        Class cls = Class.forName(endPointSnitchClassName);
                        IEndPointSnitch snitch = (IEndPointSnitch)cls.getConstructor().newInstance();
                        if (Boolean.getBoolean("bigdata.dastor.dynamic.nodetopolocator"))
                            epSnitch = new DynamicEndpointSnitch(snitch);
                        else
                            epSnitch = snitch;
                    }
                    catch (ClassNotFoundException e)
                    {
                        throw new ConfigurationException("Invalid NodeTopoLocator class " + endPointSnitchClassName);
                    }
                    catch (NoSuchMethodException e)
                    {
                        throw new ConfigurationException("Invalid NodeTopoLocator class " + endPointSnitchClassName + " " + e.getMessage());
                    }
                    catch (InstantiationException e)
                    {
                        throw new ConfigurationException("Invalid NodeTopoLocator class " + endPointSnitchClassName + " " + e.getMessage());
                    }
                    catch (IllegalAccessException e)
                    {
                        throw new ConfigurationException("Invalid NodeTopoLocator class " + endPointSnitchClassName + " " + e.getMessage());
                    }
                    catch (InvocationTargetException e)
                    {
                        throw new ConfigurationException("Invalid NodeTopoLocator class " + endPointSnitchClassName + " " + e.getMessage());
                    }
                }
                
                // BIGDATA: Bucket Mapper
                String bucketMapperClassName = xmlUtils.getNodeValue("/Dastor/Spaces/Space[@Name='" + ksName + "']/BucketMapper");
                IBucketMapper bucketMapper = null;
                if (bucketMapperClassName != null)
                {
                    logger.info("BucketMapper of " + ksName + " is " + bucketMapperClassName);
                    try
                    {
                        Class cls = Class.forName(bucketMapperClassName);
                        bucketMapper = (IBucketMapper)cls.getConstructor().newInstance();
                    }
                    catch (ClassNotFoundException e)
                    {
                        throw new ConfigurationException("Invalid bucketMapper class " + bucketMapperClassName);
                    }
                    catch (NoSuchMethodException e)
                    {
                        throw new ConfigurationException("Invalid bucketMapper class " + bucketMapperClassName + " " + e.getMessage());
                    }
                    catch (InstantiationException e)
                    {
                        throw new ConfigurationException("Invalid bucketMapper class " + bucketMapperClassName + " " + e.getMessage());
                    }
                    catch (IllegalAccessException e)
                    {
                        throw new ConfigurationException("Invalid bucketMapper class " + bucketMapperClassName + " " + e.getMessage());
                    }
                    catch (InvocationTargetException e)
                    {
                        throw new ConfigurationException("Invalid bucketMapper class " + bucketMapperClassName + " " + e.getMessage());
                    }
                }

                String xqlTable = "/Dastor/Spaces/Space[@Name='" + ksName + "']/";
                NodeList columnFamilies = xmlUtils.getRequestedNodeList(xqlTable + "Bucket");

                KSMetaData meta = new KSMetaData(ksName, repStratClass, repFact, epSnitch, bucketMapper);

                //NodeList columnFamilies = xmlUtils.getRequestedNodeList(table, "ColumnFamily");
                int size2 = columnFamilies.getLength();

                for ( int j = 0; j < size2; ++j )
                {
                    Node columnFamily = columnFamilies.item(j);
                    String tableName = ksName;
                    String cfName = XMLUtils.getAttributeValue(columnFamily, "Name");
                    if (cfName == null)
                    {
                        throw new ConfigurationException("Bucket Name attribute is required");
                    }
                    if (cfName.contains("-"))
                    {
                        throw new ConfigurationException("Bucket Name cannot contain - ");
                    }
                    String xqlCF = xqlTable + "Bucket[@Name='" + cfName + "']/";

                    // Parse out the column type
                    String rawColumnType = XMLUtils.getAttributeValue(columnFamily, "Type");
                    if (rawColumnType != null)
                    {
                        if (rawColumnType.equals("Nested"))
                            rawColumnType = "Super";
                    }
                    String columnType = ColumnFamily.getColumnType(rawColumnType);
                    if (columnType == null)
                    {
                        throw new ConfigurationException("Bucket " + cfName + " has invalid type " + rawColumnType);
                    }
                    
                    // Parse out the column comparator
                    AbstractType comparator = getComparator(columnFamily, "CellNameType");
                    AbstractType subcolumnComparator = null;
                    if (columnType.equals("Super"))
                    {
                        subcolumnComparator = getComparator(columnFamily, "NestedCellNameType");
                    }
                    else if (XMLUtils.getAttributeValue(columnFamily, "NestedCellNameType") != null)
                    {
                        throw new ConfigurationException("NestedCellNameType is only a valid attribute on nested buckets (not regular bucket " + cfName + ")");
                    }

                    double keyCacheSize = CFMetaData.DEFAULT_KEY_CACHE_SIZE;
                    if ((value = XMLUtils.getAttributeValue(columnFamily, "KeyCacheCap")) != null)
                    {
                        keyCacheSize = FBUtilities.parseDoubleOrPercent(value);
                    }

                    double rowCacheSize = CFMetaData.DEFAULT_ROW_CACHE_SIZE;
                    if ((value = XMLUtils.getAttributeValue(columnFamily, "RowCacheCap")) != null)
                    {
                        rowCacheSize = FBUtilities.parseDoubleOrPercent(value);
                    }
                    
                    // BIGDATA:
                    long compactSkipSize = 0L;
                    if ((value = XMLUtils.getAttributeValue(columnFamily, "CompactSkipInGB")) != null)
                    {
                        compactSkipSize = Long.parseLong(value) * 1024L * 1024L * 1024L;
                    }
                    
                    // BIGDATA:
                    Compression.Algorithm compressAlgo = null;
                    if ((value = XMLUtils.getAttributeValue(columnFamily, "Compression")) != null)
                    {
                        try
                        {
                            compressAlgo = Compression.getCompressionAlgorithmByName(value);
                        }
                        catch (IllegalArgumentException e)
                        {
                            throw new ConfigurationException("Compression attribute must be either 'gz', 'lzo', or 'none' in " + ksName + ":" + cfName);
                        }
                    }
                    
                    // Parse out user-specified logical names for the various dimensions
                    // of a the column family from the config.
                    String comment = xmlUtils.getNodeValue(xqlCF + "Comment");

                    // insert it into the table dictionary.
                    String rowCacheSavePeriodString = XMLUtils.getAttributeValue(columnFamily, "RowCacheSavePeriodInSeconds");
                    String keyCacheSavePeriodString = XMLUtils.getAttributeValue(columnFamily, "KeyCacheSavePeriodInSeconds");
                    int rowCacheSavePeriod = rowCacheSavePeriodString != null ? Integer.valueOf(rowCacheSavePeriodString) : DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS;
                    int keyCacheSavePeriod = keyCacheSavePeriodString != null ? Integer.valueOf(keyCacheSavePeriodString) : DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS;
                    meta.cfMetaData.put(cfName, new CFMetaData(tableName, cfName, columnType, comparator, subcolumnComparator,
                            comment, rowCacheSize, keyCacheSize, rowCacheSavePeriod, keyCacheSavePeriod,
                            compactSkipSize, compressAlgo));
                }

                tables.put(meta.name, meta);
            }
        }
        catch (XPathExpressionException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (TransformerException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public static IAuthenticator getAuthenticator()
    {
        return authenticator;
    }

    public static boolean isThriftFramed()
    {
        return thriftFramed;
    }

    private static AbstractType getComparator(Node columnFamily, String attr) throws ConfigurationException
    {
        String compareWith = null;
        try
        {
            compareWith = XMLUtils.getAttributeValue(columnFamily, attr);
        }
        catch (TransformerException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }

        try
        {
            return FBUtilities.getComparator(compareWith);
        }
        catch (Exception e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    /**
     * Creates all storage-related directories.
     * @throws IOException when a disk problem is encountered.
     */
    public static void createAllDirectories() throws IOException
    {
        try {
            if (dataFileDirectories.length == 0)
            {
                throw new ConfigurationException("At least one DataLocation must be specified");
            }
            for ( String dataFileDirectory : dataFileDirectories )
                FileUtils.createDirectory(dataFileDirectory);
            if (logFileDirectory == null)
            {
                throw new ConfigurationException("LogLocation must be specified");
            }
            FileUtils.createDirectory(logFileDirectory);
            if (savedCachesDirectory == null)
            {
                throw new ConfigurationException("CacheLocation must be specified");
            }
            FileUtils.createDirectory(savedCachesDirectory);
        }
        catch (ConfigurationException ex) {
            logger.error("Fatal error: " + ex.getMessage());
            System.err.println("Bad configuration; failure to start service.");
            System.exit(1);
        }
        /* make sure we have a directory for each table */
        for (String dataFile : dataFileDirectories)
        {
            FileUtils.createDirectory(dataFile + File.separator + Table.SYSTEM_TABLE);
            for (String table : tables.keySet())
            {
                String oneDir = dataFile + File.separator + table;
                FileUtils.createDirectory(oneDir);
                File streamingDir = new File(oneDir, STREAMING_SUBDIR);
                if (streamingDir.exists())
                    FileUtils.deleteDir(streamingDir);
            }
        }
    }

    /**
     * Create the metadata tables. This table has information about
     * the table name and the column families that make up the table.
     * Each column family also has an associated ID which is an int.
    */
    // TODO duplicating data b/t tablemetadata and CFMetaData is confusing and error-prone
    public static void storeMetadata() throws IOException
    {
        int cfId = 0;
        Set<String> tableset = tables.keySet();

        for (String table : tableset)
        {
            Table.TableMetadata tmetadata = Table.TableMetadata.instance(table);
            if (tmetadata.isEmpty())
            {
                tmetadata = Table.TableMetadata.instance(table);
                /* Column families associated with this table */
                Map<String, CFMetaData> columnFamilies = tables.get(table).cfMetaData;

                for (String columnFamily : columnFamilies.keySet())
                {
                    tmetadata.add(columnFamily, cfId++, DatabaseDescriptor.getColumnType(table, columnFamily));
                }
            }
        }
    }

    public static int getGcGraceInSeconds()
    {
        return gcGraceInSeconds;
    }

    public static IPartitioner getPartitioner()
    {
        return partitioner;
    }

    public static IEndPointSnitch getEndPointSnitch(String table)
    {
        return tables.get(table).epSnitch;
    }

    public static Class<? extends AbstractReplicationStrategy> getReplicaPlacementStrategyClass(String table)
    {
        return tables.get(table).repStratClass;
    }
    
    public static String getJobTrackerAddress()
    {
        return jobTrackerHost;
    }
    
    public static int getColumnIndexSize()
    {
    	return columnIndexSizeInKB * 1024;
    }

    public static int getMemtableLifetimeMS()
    {
      return memtableLifetimeMs;
    }

    public static String getInitialToken()
    {
      return initialToken;
    }

    public static int getMemtableThroughput()
    {
      return memtableThroughput;
    }

    public static double getMemtableOperations()
    {
      return memtableOperations;
    }

    public static boolean getConsistencyCheck()
    {
      return doConsistencyCheck;
    }

    public static String getClusterName()
    {
        return clusterName;
    }

    public static String getConfigFileName() {
        return configFileName;
    }

    public static String getJobJarLocation()
    {
        return jobJarFileLocation;
    }
    
    public static Map<String, CFMetaData> getTableMetaData(String tableName)
    {
        assert tableName != null;
        KSMetaData ksm = tables.get(tableName);
        assert ksm != null;
        return Collections.unmodifiableMap(ksm.cfMetaData);
    }

    // BIGDATA:
    public static KSMetaData getKSMetaData(String tableName)
    {
        assert tableName != null;
        return tables.get(tableName);
    }

    /*
     * Given a table name & column family name, get the column family
     * meta data. If the table name or column family name is not valid
     * this function returns null.
     */
    public static CFMetaData getCFMetaData(String tableName, String cfName)
    {
        assert tableName != null;
        KSMetaData ksm = tables.get(tableName);
        if (ksm == null)
            return null;
        return ksm.cfMetaData.get(cfName);
    }
    
    public static String getColumnType(String tableName, String cfName)
    {
        assert tableName != null;
        CFMetaData cfMetaData = getCFMetaData(tableName, cfName);
        
        if (cfMetaData == null)
            return null;
        return cfMetaData.columnType;
    }

    public static Set<String> getTables()
    {
        return tables.keySet();
    }

    public static List<String> getNonSystemTables()
    {
        List<String> tableslist = new ArrayList<String>(tables.keySet());
        tableslist.remove(Table.SYSTEM_TABLE);
        return Collections.unmodifiableList(tableslist);
    }

    public static int getStoragePort()
    {
        return storagePort;
    }

    public static int getThriftPort()
    {
        return thriftPort;
    }

    public static int getReplicationFactor(String table)
    {
        return tables.get(table).replicationFactor;
    }

    public static long getRpcTimeout()
    {
        return rpcTimeoutInMillis;
    }

    public static int getPhiConvictThreshold()
    {
        return phiConvictThreshold;
    }

    public static int getConsistencyThreads()
    {
        return consistencyThreads;
    }

    public static int getConcurrentReaders()
    {
        return concurrentReaders;
    }

    public static int getConcurrentWriters()
    {
        return concurrentWriters;
    }

    public static long getRowWarningThreshold()
    {
        return rowWarningThreshold;
    }
    
    public static String[] getAllDataFileLocations()
    {
        return dataFileDirectories;
    }

    /**
     * Get a list of data directories for a given table
     * 
     * @param table name of the table.
     * 
     * @return an array of path to the data directories. 
     */
    public static String[] getAllDataFileLocationsForTable(String table)
    {
        String[] tableLocations = new String[dataFileDirectories.length];

        for (int i = 0; i < dataFileDirectories.length; i++)
        {
            tableLocations[i] = dataFileDirectories[i] + File.separator + table;
        }

        return tableLocations;
    }

    public synchronized static String getNextAvailableDataLocation()
    {
        String dataFileDirectory = dataFileDirectories[currentIndex];
        currentIndex = (currentIndex + 1) % dataFileDirectories.length;
        return dataFileDirectory;
    }

    public static String getLogFileLocation()
    {
        return logFileDirectory;
    }

    public static Set<InetAddress> getSeeds()
    {
        return seeds;
    }

    public static String getColumnFamilyType(String tableName, String cfName)
    {
        assert tableName != null;
        String cfType = getColumnType(tableName, cfName);
        if ( cfType == null )
            cfType = "Standard";
    	return cfType;
    }

    /*
     * Loop through all the disks to see which disk has the max free space
     * return the disk with max free space for compactions. If the size of the expected
     * compacted file is greater than the max disk space available return null, we cannot
     * do compaction in this case.
     */
    public static String getDataFileLocationForTable(String table, long expectedCompactedFileSize)
    {
      long maxFreeDisk = 0;
      int maxDiskIndex = 0;
      String dataFileDirectory = null;
      String[] dataDirectoryForTable = getAllDataFileLocationsForTable(table);

      for ( int i = 0 ; i < dataDirectoryForTable.length ; i++ )
      {
        File f = new File(dataDirectoryForTable[i]);
        if( maxFreeDisk < f.getUsableSpace())
        {
          maxFreeDisk = f.getUsableSpace();
          maxDiskIndex = i;
        }
      }
      // Load factor of 0.9 we do not want to use the entire disk that is too risky.
      maxFreeDisk = (long)(0.9 * maxFreeDisk);
      if( expectedCompactedFileSize < maxFreeDisk )
      {
        dataFileDirectory = dataDirectoryForTable[maxDiskIndex];
        currentIndex = (maxDiskIndex + 1 )%dataDirectoryForTable.length ;
      }
      else
      {
        currentIndex = maxDiskIndex;
      }
        return dataFileDirectory;
    }
    
    public static AbstractType getComparator(String tableName, String cfName)
    {
        assert tableName != null;
        CFMetaData cfmd = getCFMetaData(tableName, cfName);
        if (cfmd == null)
            throw new NullPointerException("Unknown ColumnFamily " + cfName + " in keyspace " + tableName);
        return cfmd.comparator;
    }

    public static AbstractType getSubComparator(String tableName, String cfName)
    {
        assert tableName != null;
        return getCFMetaData(tableName, cfName).subcolumnComparator;
    }

    /**
     * @return The absolute number of keys that should be cached per table.
     */
    public static int getKeysCachedFor(String tableName, String columnFamilyName, long expectedKeys)
    {
        CFMetaData cfm = getCFMetaData(tableName, columnFamilyName);
        double v = (cfm == null) ? CFMetaData.DEFAULT_KEY_CACHE_SIZE : cfm.keyCacheSize;
        return (int)Math.min(FBUtilities.absoluteFromFraction(v, expectedKeys), Integer.MAX_VALUE);
    }

    /**
     * @return The absolute number of rows that should be cached for the columnfamily.
     */
    public static int getRowsCachedFor(String tableName, String columnFamilyName, long expectedRows)
    {
        CFMetaData cfm = getCFMetaData(tableName, columnFamilyName);
        double v = (cfm == null) ? CFMetaData.DEFAULT_ROW_CACHE_SIZE : cfm.rowCacheSize;
        return (int)Math.min(FBUtilities.absoluteFromFraction(v, expectedRows), Integer.MAX_VALUE);
    }

    public static InetAddress getListenAddress()
    {
        return listenAddress;
    }
    
    public static InetAddress getThriftAddress()
    {
        return thriftAddress;
    }

    public static double getCommitLogSyncBatchWindow()
    {
        return commitLogSyncBatchMS;
    }

    public static int getCommitLogSyncPeriod() {
        return commitLogSyncPeriodMS;
    }

    public static CommitLogSync getCommitLogSync()
    {
        return commitLogSync;
    }

    public static DiskAccessMode getDiskAccessMode()
    {
        return diskAccessMode;
    }

    public static DiskAccessMode getIndexAccessMode()
    {
        return indexAccessMode;
    }

    public static double getFlushDataBufferSizeInMB()
    {
        return flushDataBufferSizeInMB;
    }

    public static double getFlushIndexBufferSizeInMB()
    {
        return flushIndexBufferSizeInMB;
    }

    public static int getIndexedReadBufferSizeInKB()
    {
        return columnIndexSizeInKB;
    }

    public static int getSlicedReadBufferSizeInKB()
    {
        return slicedReadBufferSizeInKB;
    }

    public static int getBMTThreshold()
    {
        return bmtThreshold;
    }

    public static boolean isSnapshotBeforeCompaction()
    {
        return snapshotBeforeCompaction;
    }

    public static boolean isAutoBootstrap()
    {
        return autoBootstrap;
    }

    public static boolean hintedHandoffEnabled()
    {
        return hintedHandoffEnabled;
    }

    public static int getIndexInterval()
    {
        return indexinterval;
    }

    /**
     * BIGDATA:
     * @return The byte size of SSTable which will be skipped in compaction.
     */
    public static long getCompactSkipSize(String tableName, String columnFamilyName)
    {
        CFMetaData cfm = getCFMetaData(tableName, columnFamilyName);
        assert cfm != null;
        return cfm.compactSkipSize;
    }
    
    /**
     * BIGDATA:
     * @return The compression algorithm.
     */
    public static Compression.Algorithm getCompressAlgo(String tableName, String columnFamilyName)
    {
        CFMetaData cfm = getCFMetaData(tableName, columnFamilyName);
        assert cfm != null;
        return cfm.compressAlgo;
    }

    /**
     * BIGDATA:
     * @return true if the concurrent compaction is enabled.
     */
    public static boolean isConcCompactionEnabled()
    {
        return concurrentCompactionEnabled;
    }
    
    /**
     * BIGDATA:
     * @return true if the index at end of row for new row format.
     */
    public static boolean isNewRowFormatIndexAtEnd()
    {
        return newRowFormatIndexAtEnd;
    }

    /**
     * BIGDATA:
     */
    public static int getCompressStartRowSize()
    {
        return compressStartRowSize;
    }
    
    /**
     * BIGDATA:
     */
    public static boolean isTryLockMemoryEnabled()
    {
        return tryLockMemoryEnabled;
    }
    
    /**
     * BIGDATA:
     * @return The CFC begin time.
     */
    public static String getCFCBeginTime()
    {
        return cfcBeginTime;
    }

    /**
     * BIGDATA:
     * @return The CFC end time.
     */
    public static String getCFCEndTime()
    {
        return cfcEndTime;
    }
    
    /**
     * BIGDATA:
     * @return ture if table/keyspace is a system one.
     */
    public static boolean isSystemTable(String ksName)
    {
        return (ksName.equals(Table.SYSTEM_TABLE) || ksName.equals(Collector.ClsSystem_KS));
    }
    
}
