package com.bigdata.dastor.tools;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;


import org.apache.commons.cli.*;

import com.bigdata.dastor.cache.JMXInstrumentedCacheMBean;
import com.bigdata.dastor.concurrent.IExecutorMBean;
import com.bigdata.dastor.config.DatabaseDescriptor;
import com.bigdata.dastor.db.ColumnFamilyStoreMBean;
import com.bigdata.dastor.db.CompactionManager;
import com.bigdata.dastor.db.CompactionManagerMBean;
import com.bigdata.dastor.db.HintedHandOffManagerMBean;
import com.bigdata.dastor.dht.Range;
import com.bigdata.dastor.io.util.FileUtils;
import com.bigdata.dastor.service.StorageProxyMBean;
import com.bigdata.dastor.utils.DiskSpaceLoad;

public class DastorAdminCli {
    private static final String HOST_OPT_LONG = "host";
    private static final String HOST_OPT_SHORT = "h";
    private static final String PORT_OPT_LONG = "port";
    private static final String PORT_OPT_SHORT = "p";
    private static final int defaultPort = 8081;
    private static Options options = null;
    
    private NodeProbe probe = null;
    private String host;
    private int port;
    
    static
    {
        options = new Options();
        Option optHost = new Option(HOST_OPT_SHORT, HOST_OPT_LONG, true, "node hostname or ip address");
        optHost.setRequired(true);
        options.addOption(optHost);
        options.addOption(PORT_OPT_SHORT, PORT_OPT_LONG, true, "remote jmx agent port number");
    }
    
    public DastorAdminCli(NodeProbe probe, String host, int port)
    {
        this.probe = probe;
        this.host = host;
        this.port = port;
    }
    
    /**
     * Prints usage information to stdout.
     */
    public static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(120);
        String footer = String.format(
            "\n-- Basic monitoring commands: " +
            "\n topo         - the topo of the cluster, by this node known." +
            "\n info         - the summary info of this node." +
            "\n bktstats     - the schema and stats of bucket(s): <KS> [BKT...]" +
            "\n sysstats     - the schema and stats of system spaces." +
            "\n pxystats     - the stats of this node as a proxy." +
            "\n keydist      - the distribution (replication nodes) of a key: <KS> <KEY>" +
            "\n thstats      - the stats of thread-pools." +
            "\n streams      - the streaming files to all other nodes or specified node: [node]" +
            "\n cmstats      - the stats of compaction." +
            "\n ddstats      - the stats of deputy transfer." +
            "\n -" +

            "\n-- Basic operation commands: " +
            "\n flush        - force flush memtables of bucket(s): <KS> [BKT...]" +
            "\n repair       - force check and repair difference of replicas: <KS> [BKT...]" +
            "\n compact      - force compact data: <THRESHOLD> <KS> [BKT...]" +
            "\n cleanup      - force clean data that do not belong this node: <KS> [BKT...]" +
            "\n gc           - force garbadge collection (to delete compacted-sstables)." +
            "\n dlvhints     - force deliver hints to one node: <HOST>" +
            "\n -" +
            
            // "\n-- Advanced operation commands for experts! : " + 
            // "\n drain        - shuts node off to writes, empties memtables and the commit log." +
            // "\n decommission - decommission this node from cluster." +
            // "\n move         - move the node to new position or find a new position to boot to according to load." +
            // "\n loadbalance  - load banance all nodes." +
            // "\n removepcode  - remove position code." +    
            // "\n snapshot     - takes a snapshot for every table: [snapshotname]" +
            // "\n csnapshot    - clear all snapshots." +
            // "\n " +
            
            "\n-- Advanced Cluster-Wide operation commands: " +
            "\n gresetbkt - graceful reset a bucket in cluster, the name is user-defined: [undo]" +
            
            // "\n gsnapshot    - takes a snapshot for every table in cluster: [snapshotname]" +
            // "\n gcsnapshot   - clear all snapshots in cluster." +
            
            // "\n\n-- Debug commands for experts! : " + 
            // "\n setcachecap  - set cache capacity. <KS> <BKT> <keyCacheCap> <rowCacheCap> " +
            // "\n setcmthresh  - set the compaction threshold." +
            // "\n getcmthresh  - get the compaction threshold." +

            // 
            "\n -");
        String usage = String.format("Args: -h <host> -p <port> <command> <args>%n");
        hf.printHelp(usage, "", options, footer);
    }
    
    /**
     * Write a textual representation of the Dastor ring.
     * 
     * @param outs the stream to write to
     */
    public void printRing(PrintStream outs)
    {
        Map<Range, List<String>> rangeMap = probe.getRangeToEndPointMap(null);
        List<Range> ranges = new ArrayList<Range>(rangeMap.keySet());
        Collections.sort(ranges);
        Set<String> liveNodes = probe.getLiveNodes();
        Set<String> deadNodes = probe.getUnreachableNodes();
        Map<String, String> loadMap = probe.getLoadMap();

        // Print range-to-endpoint mapping
        int counter = 0;
        outs.print(String.format("%-14s", "PrimaryNode"));
        outs.print(String.format("%-11s", "Status"));
        outs.print(String.format("%-14s", "Load"));
        outs.print(String.format("%-43s", "PositionCode"));
        outs.println(" Replicas");
        // emphasize that we're showing the right part of each range
        if (ranges.size() > 1)
        {
            outs.println(String.format("%-14s%-11s%-14s%-43s", "", "", "", ranges.get(0).left));
        }
        // normal range & node info
        for (Range range : ranges) {
            List<String> endpoints = rangeMap.get(range);
            String primaryEndpoint = endpoints.get(0);

            outs.print(String.format("%-14s", primaryEndpoint));

            String status = liveNodes.contains(primaryEndpoint)
                          ? "Up"
                          : deadNodes.contains(primaryEndpoint)
                            ? "Down"
                            : "?";
            outs.print(String.format("%-11s", status));

            String load = loadMap.containsKey(primaryEndpoint) ? loadMap.get(primaryEndpoint) : "?";
            outs.print(String.format("%-14s", load));

            outs.print(String.format("%-43s", range.right));

            for (int i = 1; i < endpoints.size(); i++)
            {
                String ep = endpoints.get(i);
                String epsta = liveNodes.contains(ep) 
                                ? "u"
                                : deadNodes.contains(primaryEndpoint)
                                    ? "d"
                                    : "?";
                outs.print(" " + ep + "[" + epsta + "]");
            }
            outs.println("");
            
            /*
            String asciiRingArt;
            if (counter == 0)
            {
                asciiRingArt = "|<--|";
            }
            else if (counter == (rangeMap.size() - 1))
            {
                asciiRingArt = "|-->|";
            }
            else
            {
                if ((rangeMap.size() > 4) && ((counter % 2) == 0))
                    asciiRingArt = "v   |";
                else if ((rangeMap.size() > 4) && ((counter % 2) != 0))
                    asciiRingArt = "|   ^";
                else
                    asciiRingArt = "|   |";
            }
            outs.println(asciiRingArt);
            
            counter++;
            */
        }
    }
    
    public void printThreadPoolStats(PrintStream outs)
    {
        outs.print(String.format("%-25s", "Pool Name"));
        outs.print(String.format("%10s", "Active"));
        outs.print(String.format("%10s", "Pending"));
        outs.print(String.format("%15s", "Completed"));
        outs.println();
        
        Iterator<Map.Entry<String, IExecutorMBean>> threads = probe.getThreadPoolMBeanProxies();
        
        for (; threads.hasNext();)
        {
            Map.Entry<String, IExecutorMBean> thread = threads.next();
            String poolName = thread.getKey();
            IExecutorMBean threadPoolProxy = thread.getValue();
            outs.print(String.format("%-25s", poolName));
            outs.print(String.format("%10d", threadPoolProxy.getActiveCount()));
            outs.print(String.format("%10d", threadPoolProxy.getPendingTasks()));
            outs.print(String.format("%15d", threadPoolProxy.getCompletedTasks()));
            outs.println();
        }
    }

    /**
     * Write node information.
     * 
     * @param outs the stream to write to
     */
    public void printInfo(PrintStream outs)
    {
        outs.println(String.format("%-17s: %s", "Who",  host + ":" + port));
        outs.println(String.format("%-17s: %s", "Status", probe.getOperationMode()));
        outs.println(String.format("%-17s: %s", "PositionCode", probe.getToken()));
        outs.println(String.format("%-17s: %s", "Generation", probe.getCurrentGenerationNumber()));
        
        // Uptime
        long secondsUp = probe.getUptime() / 1000;
        outs.println(String.format("%-17s: %d", "Uptime (seconds)", secondsUp));

        // Memory usage
        MemoryUsage heapUsage = probe.getHeapMemoryUsage();
        double memUsed = (double)heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double)heapUsage.getMax() / (1024 * 1024);
        outs.println(String.format("%-17s: %.2f / %.2f", "Heap Memory (MB)", memUsed, memMax));
        
        // Disk space
        outs.println(String.format("%-17s: %s / %s", "Load", probe.getLoadString(), 
                probe.getStorageServiceMBean().getGrossLoadString()));
        Map<String, DiskSpaceLoad> loadMap = probe.getStorageServiceMBean().getTablesDiskSpaceLoad();
        for (Entry<String, DiskSpaceLoad> entry : loadMap.entrySet())
        {
            outs.println(String.format("%-17s : %s / %s", "Load of "+entry.getKey(),
                    FileUtils.stringifyFileSize(entry.getValue().net), 
                    FileUtils.stringifyFileSize(entry.getValue().gross)));
        }
    }

    public void printStreamInfo(final InetAddress addr, PrintStream outs)
    {
        outs.println(String.format("Mode: %s", probe.getOperationMode()));
        Set<InetAddress> hosts = addr == null ? probe.getStreamDestinations() : new HashSet<InetAddress>(){{add(addr);}};
        if (hosts.size() == 0)
            outs.println("Not sending any streams.");
        for (InetAddress host : hosts)
        {
            try
            {
                List<String> files = probe.getFilesDestinedFor(host);
                if (files.size() > 0)
                {
                    outs.println(String.format("Streaming to: %s", host));
                    for (String file : files)
                        outs.println(String.format("   %s", file));
                }
                else
                {
                    outs.println(String.format(" Nothing streaming to %s", host));
                }
            }
            catch (IOException ex)
            {
                outs.println(String.format("   Error retrieving file data for %s", host));
            }
        }

        hosts = addr == null ? probe.getStreamSources() : new HashSet<InetAddress>(){{add(addr); }};
        if (hosts.size() == 0)
            outs.println("Not receiving any streams.");
        for (InetAddress host : hosts)
        {
            try
            {
                List<String> files = probe.getIncomingFiles(host);
                if (files.size() > 0)
                {
                    outs.println(String.format("Streaming from: %s", host));
                    for (String file : files)
                        outs.println(String.format("   %s", file));
                }
                else
                {
                    outs.println(String.format(" Nothing streaming from %s", host));
                }
            }
            catch (IOException ex)
            {
                outs.println(String.format("   Error retrieving file data for %s", host));
            }
        }
    }
    
    public void printColumnFamilyStats(PrintStream outs, String keyspace, boolean printSys)
    {
        Map <String, List <ColumnFamilyStoreMBean>> cfstoreMap = new HashMap <String, List <ColumnFamilyStoreMBean>>();

        // get a list of column family stores
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies;
        if (keyspace == null)
        {
            cfamilies = probe.getColumnFamilyStoreMBeanProxies();
        }
        else
        {
            cfamilies = probe.getColumnFamilyStoreMBeanProxies(keyspace);
        }

        for (;cfamilies.hasNext();)
        {
            Map.Entry<String, ColumnFamilyStoreMBean> entry = cfamilies.next();
            String tableName = entry.getKey();
            ColumnFamilyStoreMBean cfsProxy = entry.getValue();

            if (!cfstoreMap.containsKey(tableName))
            {
                List<ColumnFamilyStoreMBean> columnFamilies = new ArrayList<ColumnFamilyStoreMBean>();
                columnFamilies.add(cfsProxy);
                cfstoreMap.put(tableName, columnFamilies);
            }
            else
            {
                cfstoreMap.get(tableName).add(cfsProxy);
            }
        }

        // print out the table statistics
        for (Entry<String, List<ColumnFamilyStoreMBean>> entry : cfstoreMap.entrySet())
        {
            String tableName = entry.getKey();
            if (keyspace == null)
            {
                if (printSys)
                {
                    if (!DatabaseDescriptor.isSystemTable(tableName))
                        continue;
                }
                else
                {
                    if (DatabaseDescriptor.isSystemTable(tableName))
                        continue;
                }
            }
            
            List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
            long tableReadCount = 0;
            long tableWriteCount = 0;
            int tablePendingTasks = 0;
            double tableTotalReadTime = 0.0f;
            double tableTotalWriteTime = 0.0f;

            outs.println("Space: " + tableName);
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                long writeCount = cfstore.getWriteCount();
                long readCount = cfstore.getReadCount();

                if (readCount > 0)
                {
                    tableReadCount += readCount;
                    tableTotalReadTime += cfstore.getTotalReadLatencyMicros();
                }
                if (writeCount > 0)
                {
                    tableWriteCount += writeCount;
                    tableTotalWriteTime += cfstore.getTotalWriteLatencyMicros();
                }
                tablePendingTasks += cfstore.getPendingTasks();
            }

            double tableReadLatency = tableReadCount > 0 ? tableTotalReadTime / tableReadCount / 1000 : Double.NaN;
            double tableWriteLatency = tableWriteCount > 0 ? tableTotalWriteTime / tableWriteCount / 1000 : Double.NaN;

            outs.println("\tRead Count: " + tableReadCount);
            outs.println("\tRead Latency: " + String.format("%s", tableReadLatency) + " ms.");
            outs.println("\tWrite Count: " + tableWriteCount);
            outs.println("\tWrite Latency: " + String.format("%s", tableWriteLatency) + " ms.");
            outs.println("\tPending Tasks: " + tablePendingTasks);

            // print out column family statistics for this table
            for (ColumnFamilyStoreMBean cfstore : columnFamilies)
            {
                printColumnFamilyStats(outs, tableName, cfstore);
            }
            outs.println("----------------");
        }
    }
    
    public void printColumnFamilyStats(PrintStream outs, String tableName, ColumnFamilyStoreMBean cfstore)
    {
        String cfName = cfstore.getColumnFamilyName();
        outs.println("\t\tBucket Name: " + cfName);
        outs.println("\t\tSSTable count: " + cfstore.getLiveSSTableCount());
        
        outs.println("\t\tStorage used (live): " + cfstore.getLiveDiskSpaceUsed());
        outs.println("\t\tStorage used (gross): " + cfstore.getTotalDiskSpaceUsed());
        
        outs.println("\t\tMemtable cells count: " + cfstore.getMemtableColumnsCount());
        outs.println("\t\tMemtable data size: " + cfstore.getMemtableDataSize());
        outs.println("\t\tMemtable flush count: " + cfstore.getMemtableSwitchCount());
        
        outs.println("\t\tTotal Read count: " + cfstore.getReadCount());
        outs.println("\t\tRecent Read latency(ms): " + String.format("%01.3f", cfstore.getRecentReadLatencyMicros() / 1000));
        outs.println("\t\tRecent Read throughput(ops/s): " + cfstore.getRecentReadThroughput());
        outs.println("\t\tTotal Write count: " + cfstore.getWriteCount());
        outs.println("\t\tRecent Write latency(ms): " + String.format("%01.3f", cfstore.getRecentWriteLatencyMicros() / 1000));
        outs.println("\t\tRecent Write throughput(ops/s): " + cfstore.getRecentWriteThroughput());
        outs.println("\t\tPending tasks: " + cfstore.getPendingTasks());

        JMXInstrumentedCacheMBean keyCacheMBean = probe.getKeyCacheMBean(tableName, cfName);
        if (keyCacheMBean.getCapacity() > 0)
        {
            outs.println("\t\tKey cache capacity: " + keyCacheMBean.getCapacity());
            outs.println("\t\tKey cache size: " + keyCacheMBean.getSize());
            outs.println("\t\tKey cache hit rate: " + keyCacheMBean.getRecentHitRate());
        }
        else
        {
            outs.println("\t\tKey cache: disabled");
        }

        JMXInstrumentedCacheMBean rowCacheMBean = probe.getRowCacheMBean(tableName, cfName);
        if (rowCacheMBean.getCapacity() > 0)
        {
            outs.println("\t\tRow cache capacity: " + rowCacheMBean.getCapacity());
            outs.println("\t\tRow cache size: " + rowCacheMBean.getSize());
            outs.println("\t\tRow cache hit rate: " + rowCacheMBean.getRecentHitRate());
        }
        else
        {
            outs.println("\t\tRow cache: disabled");
        }

        outs.println("\t\tCompacted row minimum size: " + cfstore.getMinRowCompactedSize());
        outs.println("\t\tCompacted row maximum size: " + cfstore.getMaxRowCompactedSize());
        outs.println("\t\tCompacted row mean size: " + cfstore.getMeanRowCompactedSize());
        
        outs.println("\t\tStatus: " + cfstore.getStatusString());
        Date cfStatusTimestamp = new Date();
        cfStatusTimestamp.setTime(cfstore.getStatusTimestamp());
        outs.println("\t\tStatus timestamp: " + cfStatusTimestamp.getTime() + ", " + cfStatusTimestamp);
        outs.println("");
    }

    // BIGDATA
    public void printProxyStats(PrintStream outs)
    {
        StorageProxyMBean spMBean = probe.getStorageProxyMBean();
        outs.println("Read Operation Count : " + spMBean.getReadOperations());
        outs.println("Total  Read Latency(ms) : " + (double)spMBean.getTotalReadLatencyMicros()/1000);
        outs.println("Recent Read Latency(ms) : " + spMBean.getRecentReadLatencyMicros()/1000);
        outs.println("Recent Read Throughput(ops/sec) : " + spMBean.getRecentReadThroughput());
        
        outs.println("Range Read Operation Count: " + spMBean.getRangeOperations());
        outs.println("Total  Range Read Latency(ms): " + (double)spMBean.getTotalRangeLatencyMicros()/1000);
        outs.println("Recent Range Read Latency(ms): " + spMBean.getRecentRangeLatencyMicros()/1000);
        outs.println("Recent Range Read Throughput(ops/sec) : " + spMBean.getRecentRangeThroughput());

        outs.println("Write Operation Count: " + spMBean.getWriteOperations());
        outs.println("Total  Write Latency(ms): " + (double)spMBean.getTotalWriteLatencyMicros()/1000);
        outs.println("Recent Write Latency(ms): " + spMBean.getRecentWriteLatencyMicros()/1000);
        outs.println("Recent Write Throughput(ops/sec) : " + spMBean.getRecentWriteThroughput());
    }
    
    public void printCompactionStats(PrintStream outs)
    {
        CompactionManagerMBean cmProxy = probe.getCmMBean();
        outs.println("Min Compaction Threshold :" + cmProxy.getMinimumCompactionThreshold());
        outs.println("Max Compaction Threshold :" + cmProxy.getMaximumCompactionThreshold());
        outs.println("Bucket in Compacting :" + cmProxy.getColumnFamilyInProgress());
        outs.println("Bytes in Compacting :" + cmProxy.getBytesTotalInProgress());
        outs.println("Bytes in Compacted :" + cmProxy.getBytesCompacted());
        outs.println("Pending Tasks :" + cmProxy.getPendingTasks());
    }
    
    // BIGDATA:
    public void printHhStats(PrintStream outs)
    {
        HintedHandOffManagerMBean hhProxy = probe.getHhProxyMBean();
        outs.println("Delivered rows : " + hhProxy.getDeliveredRows());
        outs.println("Delivering to endpoint : " + hhProxy.getDeliveringEp());
        outs.println("Delivering for table : " + hhProxy.getDeliveringTable());
        outs.println("Delivering for bucket : " + hhProxy.getDeliveringCf());
    }
    
    // BIGDATA:
    public void requestGC()
    {
        probe.getStorageServiceMBean().requestGC();
    }
    
    // BIGDATA
    public void deliverHints(String endPoint) throws UnknownHostException
    {
        probe.getStorageServiceMBean().deliverHints(endPoint);
    }
    
    // BIGDATA:
    public void closeProbe()
    {
        if (probe != null)
        {
            probe.close();
        }
        probe = null;
    }
    
    // BIGDATA:
    public void setProbe(NodeProbe probe, String host, int port)
    {
        if (probe != null)
            closeProbe();
        this.probe = probe;
        this.host = host;
        this.port = port;
    }
    
    // BIGDATA:
    public NodeProbe getProbe()
    {
        return probe;
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ParseException
    {
        ArrayList<String> cmdArgList = new ArrayList<String>();
        DastorAdminCli nodeCmd = DastorAdminCli.initCli(args, cmdArgList);
        
        String[] arguments = cmdArgList.toArray(new String[cmdArgList.size()]);
        
        int ret = nodeCmd.executeCommand(arguments);
        
        System.exit(ret);
    }

    public static DastorAdminCli initCli(String[] args, ArrayList<String> cmdArgList) throws IOException, InterruptedException, ParseException
    {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException parseExcep)
        {
            System.err.println(parseExcep);
            printUsage();
            System.exit(1);
        }

        String host = cmd.getOptionValue(HOST_OPT_LONG);
        int port = defaultPort;
        
        String portNum = cmd.getOptionValue(PORT_OPT_LONG);
        if (portNum != null)
        {
            try
            {
                port = Integer.parseInt(portNum);
            }
            catch (NumberFormatException e)
            {
                throw new ParseException("Port must be a number");
            }
        }
        
        NodeProbe probe = null;
        try
        {
            probe = new NodeProbe(host, port);
        }
        catch (IOException ioe)
        {
            System.err.println("Error connecting to remote JMX agent!");
            ioe.printStackTrace();
            System.exit(3);
        }
        System.out.println("connected to " + host + ":" + port);
                
        for (String arg :  cmd.getArgs())
        {
            cmdArgList.add(arg);
        }
        
        return new DastorAdminCli(probe, host, port);
    }
    
    public int executeCommand(String[] arguments) throws IOException, InterruptedException
    {
        DastorAdminCli nodeCmd = this;
        
        if (arguments.length < 1)
        {
            System.err.println("Missing argument for command.");
            printUsage();
            return 1;
        }
        
        String cmdName = arguments[0];
        if (cmdName.equals("topo"))
        {
            nodeCmd.printRing(System.out);
        }
        else if (cmdName.equals("info"))
        {
            nodeCmd.printInfo(System.out);
        }
        else if (cmdName.equals("cleanup"))
        {
            if (arguments.length >= 2)
            {
                String[] columnFamilies = new String[arguments.length - 2];
                for (int i = 0; i < columnFamilies.length; i++)
                {
                    columnFamilies[i] = arguments[i + 2];
                }
                probe.forceTableCleanup(arguments[1], columnFamilies);
            }
            else
            {
                probe.forceTableCleanup();
            }
        }
        else if (cmdName.equals("compact"))
        {
            if (arguments.length >= 3)
            {
                String[] columnFamilies = new String[arguments.length - 3];
                for (int i = 0; i < columnFamilies.length; i++)
                {
                    columnFamilies[i] = arguments[i + 3];
                }
                probe.forceTableCompaction(Integer.parseInt(arguments[1]), arguments[2], columnFamilies);
            }
            else if (arguments.length == 2)
            {
                // compact all keyspaces.
                probe.forceTableCompaction(Integer.parseInt(arguments[1]));
            }
            else
            {
                probe.forceTableCompaction(1);
            }
        }
        else if (cmdName.equals("bktstats"))
        {
            try
            {
                if (arguments.length >= 3)
                {
                    // print the specified cf
                    ColumnFamilyStoreMBean cfmbean = probe.getColumnFamilyMBean(arguments[1], arguments[2]);
                    nodeCmd.printColumnFamilyStats(System.out, arguments[1], cfmbean);
                }
                else if (arguments.length == 2)
                {
                    // print cfs of the specified ks
                    nodeCmd.printColumnFamilyStats(System.out, arguments[1], false);
                }
                else
                {
                    // print all cfs of all tables
                    nodeCmd.printColumnFamilyStats(System.out, null, false);
                }
            }
            catch (Exception e)
            {
                System.err.println("Maybe wrong arguments.");
                e.printStackTrace();
                return 3;
            }
        }
        else if (cmdName.equals("sysstats"))
        {
            nodeCmd.printColumnFamilyStats(System.out, null, true);
        }
        else if (cmdName.equals("decommission"))
        {
            probe.decommission();
        }
        else if (cmdName.equals("loadbalance"))
        {
            probe.loadBalance();
        }
        else if (cmdName.equals("move"))
        {
            if (arguments.length <= 1)
            {
                System.err.println("missing position code argument");
                printUsage();
                return 1;
            }
            probe.move(arguments[1]);
        }
        else if (cmdName.equals("removepcode"))
        {
            if (arguments.length <= 1)
            {
                System.err.println("missing position code argument");
                printUsage();
                return 1;
            }
            probe.removeToken(arguments[1]);
        }
        else if (cmdName.equals("snapshot"))
        {
            String snapshotName = "";
            if (arguments.length > 1)
            {
                snapshotName = arguments[1];
            }
            probe.takeSnapshot(snapshotName);
        }
        else if (cmdName.equals("csnapshot"))
        {
            probe.clearSnapshot();
        }
        else if (cmdName.equals("thstats"))
        {
            nodeCmd.printThreadPoolStats(System.out);
        }
        else if (cmdName.equals("flush") || cmdName.equals("repair"))
        {
            if (arguments.length < 2)
            {
                System.err.println("Missing space name argument.");
                printUsage();
                return 1;
            }

            String[] columnFamilies = new String[arguments.length - 2];
            for (int i = 0; i < columnFamilies.length; i++)
            {
                columnFamilies[i] = arguments[i + 2];
            }
            if (cmdName.equals("flush"))
                probe.forceTableFlush(arguments[1], columnFamilies);
            else // cmdName.equals("repair")
                probe.forceTableRepair(arguments[1], columnFamilies);
        }
        else if (cmdName.equals("drain"))
        {
            try 
            {
                probe.drain();
            } catch (ExecutionException ee) 
            {
                System.err.println("Error occured during flushing");
                ee.printStackTrace();
                return 3;
            }
        }
        else if (cmdName.equals("setcachecap"))
        {
            if (arguments.length != 5)
            {
                System.err.println("cacheinfo requires space and bucket name arguments, followed by key cache capacity and row cache capacity, in rows");
                printUsage();
                return 1;
            }
            String tableName = arguments[1];
            String cfName = arguments[2];
            int keyCacheCapacity = Integer.valueOf(arguments[3]);
            int rowCacheCapacity = Integer.valueOf(arguments[4]);
            probe.setCacheCapacities(tableName, cfName, keyCacheCapacity, rowCacheCapacity);
        }
        else if (cmdName.equals("getcmthresh"))
        {
            probe.getCompactionThreshold(System.out);
        }
        else if (cmdName.equals("setcmthresh"))
        {
            if (arguments.length < 2)
            {
                System.err.println("Missing threshold value(s)");
                printUsage();
                return 1;
            }
            int minthreshold = Integer.parseInt(arguments[1]);
            int maxthreshold = CompactionManager.instance.getMaximumCompactionThreshold();
            if (arguments.length > 2)
            {
                maxthreshold = Integer.parseInt(arguments[2]);
            }

            if (minthreshold > maxthreshold)
            {
                System.err.println("Min threshold can't be greater than Max threshold");
                printUsage();
                return 1;
            }

            if (minthreshold < 2 && maxthreshold != 0)
            {
                System.err.println("Min threshold must be at least 2");
                printUsage();
                return 1;
            }
            probe.setCompactionThreshold(minthreshold, maxthreshold);
        }
        else if (cmdName.equals("streams"))
        {
            String otherHost = arguments.length > 1 ? arguments[1] : null;
            nodeCmd.printStreamInfo(otherHost == null ? null : InetAddress.getByName(otherHost), System.out);
        }
        else if (cmdName.equals("pxystats"))
        {
            nodeCmd.printProxyStats(System.out);
        }
        else if (cmdName.equals("cmstats"))
        {
            nodeCmd.printCompactionStats(System.out);
        }
        else if (cmdName.equals("ddstats"))
        {
            nodeCmd.printHhStats(System.out);
        }
        else if (cmdName.equals("gc"))
        {
            nodeCmd.requestGC();
        }
        else if (cmdName.equals("dlvhints"))
        {
            if (arguments.length < 2)
            {
                System.err.println("Missing node host/ipaddr!");
                printUsage();
                return 1;
            }
            nodeCmd.deliverHints(arguments[1]);
        }
        
        // Following commands for cluster wide.
        else if (cmdName.equals("gsnapshot"))
        {
            String snapshotName = "";
            if (arguments.length > 1)
            {
                snapshotName = arguments[1];
            }
            nodeCmd.takeGlobalSnapshot(snapshotName);
        }
        else if (cmdName.equals("gcsnapshot"))
        {
            nodeCmd.clearGlobalSnapshot();
        }
        else if (cmdName.equals("gresetbkt"))
        {
            if (arguments.length < 3)
            {
                System.err.println("Missing space or bucket name arguments.");
                printUsage();
                return 1;
            }
            try
            {
                if ((arguments.length >= 4) && arguments[3].equals("undo"))
                    probe.getStorageProxyMBean().graceResetClusterCFUndo(arguments[1], arguments[2]);
                else
                    probe.getStorageProxyMBean().graceResetClusterCF(arguments[1], arguments[2]);
            }
            catch (Exception e)
            {
                System.err.println("Error occured during reseting!");
                e.printStackTrace();
                return 3;
            }
        }
        else if (cmdName.equals("keydist"))
        {
            if (arguments.length < 3)
            {
                System.err.println("Missing space or key arguments.");
                printUsage();
                return 1;
            }
            List<InetAddress> eps = probe.getEndPoints(arguments[1], arguments[2]);
            for (InetAddress ep : eps)
            {
                System.out.println(ep);
            }
        }

        else
        {
            System.err.println("Unrecognized command: " + cmdName + ".");
            printUsage();
            return 1;
        }
        
        return 0;
    }
    
    // Following methods for cluster wide.
    
    /**
     * Take a snapshot of all tables on all (live) nodes in the cluster
     *
     * @param snapshotName name of the snapshot
     */
    public void takeGlobalSnapshot(String snapshotName) throws IOException, InterruptedException
    {
        Set<String> liveNodes = probe.getLiveNodes();
        try
        {
            probe.takeSnapshot(snapshotName);
            System.out.println(host + " snapshot taken");
        }
        catch (IOException e)
        {
            System.out.println(host + " snapshot FAILED!: " + e.getMessage());
        }

        liveNodes.remove(this.host);
        for (String liveNode : liveNodes)
        {
            try
            {
                this.host = liveNode;
                probe = new NodeProbe(host, port);
                probe.takeSnapshot(snapshotName);
                System.out.println(host + " snapshot taken");
            }
            catch (IOException e)
            {
                System.out.println(host + " snapshot FAILED!: " + e.getMessage());
            }
        }
    }

    /**
     * Remove all the existing snapshots from all (live) nodes in the cluster
     */
    public void clearGlobalSnapshot() throws IOException, InterruptedException
    {
        Set<String> liveNodes = probe.getLiveNodes();
        try
        {
            probe.clearSnapshot();
            System.out.println(host + " snapshot cleared");
        }
        catch (IOException e)
        {
            System.out.println(host + " snapshot clear FAILED!: " + e.getMessage());
        }

        liveNodes.remove(this.host);
        for (String liveNode : liveNodes)
        {
            try
            {
                this.host = liveNode;
                probe = new NodeProbe(host, port);
                probe.clearSnapshot();
                System.out.println(host + " snapshot cleared");
            }
            catch (IOException e)
            {
                System.out.println(host + " snapshot clear FAILED!: " + e.getMessage());
            }
        }
    }
    
    // BIGDATA
    public int getPort()
    {
        return port;
    }
    
    // BIGDATA
    public String getHost()
    {
        return host;
    }
}
