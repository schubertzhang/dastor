package com.bigdata.dastor.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.bigdata.dastor.cache.JMXInstrumentedCacheMBean;
import com.bigdata.dastor.cfc.Collector;
import com.bigdata.dastor.config.CFMetaData;
import com.bigdata.dastor.config.KSMetaData;
import com.bigdata.dastor.db.ColumnFamilyStoreMBean;
import com.bigdata.dastor.db.Table;
import com.bigdata.dastor.io.util.FileUtils;
import com.bigdata.dastor.service.StorageProxyMBean;
import com.bigdata.dastor.tools.NodeProbe;
import com.bigdata.dastor.utils.DiskSpaceLoad;

public class NodeServlet extends HttpServlet{

    private static final long serialVersionUID = 4563027493722205498L;

    private static Logger logger = Logger.getLogger(ClusterServlet.class);

    /* (non-Javadoc)
     * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException 
    {
        
        String ip = req.getParameter("ip");
        String op = req.getParameter("op");
        
        String ret = null;

        if (ip == null || op == null)
        {
            logger.error("ip == null || op = null");
            Result.R r = new Result.R();
            r.buildError("ip == null || op == null");
            ObjectMapper mapper = new ObjectMapper();
            ret = mapper.writeValueAsString(r);
        }
        else if (NodeList.getInstance().getNodeProbe(ip) == null)
        {
            logger.error("the node is not alive : " + ip);
            Result.NodeInfoList r = new Result.NodeInfoList();
            r.buildError("the node is not alive : " + ip);
            ObjectMapper mapper = new ObjectMapper();
            ret = mapper.writeValueAsString(r);
        }
        else if (op.equals("info"))
        {
            logger.info("get info");
            ret = procInfo(ip);
        }
        else if (op.equals("cfstats"))
        {
            logger.info("get cfstats");
            ret = procCFState(ip);
        }
        else if (op.equals("schema"))
        {
            logger.info("get schema");
            ret = procTotalSchema(ip);
        }
        else if (op.equals("proxy"))
        {
            logger.info("get proxy");
            ret = procProxyStats(ip);
        }
        else if (op.equals("single_bucket_schema"))
        {
            logger.info("get single_bucket_schema");
            ret = procSingleBucketSchema(ip, req.getParameter("space"), req
                    .getParameter("bucket"));
        }
        else if (op.equals("single_bucket_state"))
        {
            logger.info("get single_bucket_state");
            ret = procSingleBucketState(ip, req.getParameter("space"), req
                    .getParameter("bucket"));
        }
        else if (op.equals("fsstats"))
        {
            logger.info("get single_bucket_state");
            ret = procFSState(ip);
        }
        else if (op.equals("flush"))
        {
            ret = procBucketFlush(ip, req.getParameter("space"), req
                    .getParameter("bucket"));
        }
        else if (op.equals("compact"))
        {
            ret = procBucketCompact(ip, req.getParameter("space"), req
                    .getParameter("bucket"));
        }
        else if (op.equals("reset"))
        {
            ret = procBucketReset(ip, req.getParameter("space"), req
                    .getParameter("bucket"));
        }
        else if (op.equals("undoreset"))
        {
            ret = procBucketUndoReset(ip, req.getParameter("space"), req
                    .getParameter("bucket"));
        }
        else
        {
            logger.error("error op : " + op);
            Result.R r = new Result.R();
            r.buildError("error op : " + op);
            ObjectMapper mapper = new ObjectMapper();
            ret = mapper.writeValueAsString(r);
        }
        
        logger.info("ret : "+ret);
        PrintWriter writer = resp.getWriter();
        writer.println(ret);
        writer.close();
    }

    /* (non-Javadoc)
     * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException 
    {
        doGet(req, resp);
    }

    private String procInfo(String ip) throws JsonGenerationException, JsonMappingException, IOException
    {
        NodeProbe probe = NodeList.getInstance().getNodeProbe(ip);
        ObjectMapper mapper = new ObjectMapper();

        Result.NodeInfo info = new Result.NodeInfo();
        try
        {
            info.setIp(ip);
            info.setAlive(true);
            info.setOperating(probe.getOperationMode());
            info.setGeneration(probe.getCurrentGenerationNumber());
            info.setUptime(ClusterServlet.getUptimeTimeString(probe.getUptime()));
            
            HashMap<String, Double> diskMap = new HashMap<String, Double>();
            for (FileUtils.FSInfo fsinfo : probe.getStorageFSInfo().values())
            {
                diskMap.put(fsinfo.fsName, fsinfo.totalSize);
            }
            double s = 0;
            for (Double size : diskMap.values())
            {
                s += size.doubleValue();
            }
            info.setConfiguredCapacity(FileUtils.stringifyFileSize(s));
            
            DiskSpaceLoad dsl = probe.getStorageServiceMBean().getDiskSpaceLoad();
            info.setLoad(FileUtils.stringifyFileSize(dsl.net));
            info.setGrossLoad(FileUtils.stringifyFileSize(dsl.gross));
            
            MemoryUsage heapUsage = probe.getHeapMemoryUsage();
            double memUsed = (double)heapUsage.getUsed() / (1024 * 1024);
            double memMax = (double)heapUsage.getMax() / (1024 * 1024);
            info.setHeapusage(String.format("%.2fMB/%.2fMB", memUsed, memMax));
            
            Result.NodeInfoR r = new Result.NodeInfoR();
            r.buildOK();
            r.setNode(info);
            return mapper.writeValueAsString(r);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            logger.error("get node info error : " + e);
            if (e instanceof IOException)
                NodeList.getInstance().setNodeDead(ip);

            Result.R r = new Result.R();
            r.buildError(e.toString());
            return mapper.writeValueAsString(r);
        }
    }
    
    private String procCFState(String ip) throws JsonGenerationException, JsonMappingException, IOException
    {
        NodeProbe probe = NodeList.getInstance().getNodeProbe(ip);
        ObjectMapper mapper = new ObjectMapper();

        try
        {
            Map<String, List<ColumnFamilyStoreMBean>> cfstoreMap = new HashMap<String, List<ColumnFamilyStoreMBean>>();
            Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe
                    .getColumnFamilyStoreMBeanProxies();

            for (; cfamilies.hasNext();) 
            {
                Map.Entry<String, ColumnFamilyStoreMBean> entry = cfamilies
                        .next();
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
            
            ArrayList<Result.SpaceInfo> spaceList = new ArrayList<Result.SpaceInfo>();
            
            for (Entry<String, List<ColumnFamilyStoreMBean>> entry : cfstoreMap.entrySet())
            {
                String tableName = entry.getKey();
                List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
                int tableReadCount = 0;
                int tableWriteCount = 0;
                int tablePendingTasks = 0;
                double tableTotalReadTime = 0.0f;
                double tableTotalWriteTime = 0.0f;

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

                ArrayList<Result.BucketInfo> bucketList = new ArrayList<Result.BucketInfo>();
                
                // print out column family statistics for this table
                for (ColumnFamilyStoreMBean cfstore : columnFamilies)
                {
                    Result.BucketInfo bucket = new Result.BucketInfo();
                    bucket.setName(cfstore.getColumnFamilyName());
                    bucket.setSsTableCount(cfstore.getLiveSSTableCount());
                    bucket.setSpaceUsedLive(cfstore.getLiveDiskSpaceUsed());
                    bucket.setSpaceUsedTotal(cfstore.getTotalDiskSpaceUsed());
                    bucket.setMemtableCellCount(cfstore.getMemtableColumnsCount());
                    bucket.setMemtableDataSize(cfstore.getMemtableDataSize());
                    bucket.setMemtableSwitchCount(cfstore.getMemtableSwitchCount());
                    bucket.setReadCount(cfstore.getReadCount());
                    bucket.setReadLatency(cfstore.getRecentReadLatencyMicros()/1000);
                    bucket.setReadThroughput(cfstore.getRecentReadThroughput());
                    bucket.setWriteCount(cfstore.getWriteCount());
                    bucket.setWriteLatency(cfstore.getRecentWriteLatencyMicros()/1000);
                    bucket.setWriteThroughput(cfstore.getRecentReadThroughput());
                    bucket.setPendingTasks(cfstore.getPendingTasks());
                    bucket.setCompactedRowMinimumSize(cfstore.getMinRowCompactedSize());
                    bucket.setCompactedRowMaximumSize(cfstore.getMaxRowCompactedSize());
                    bucket.setCompactedRowMeanSize(cfstore.getMeanRowCompactedSize());
                    bucket.setStatus(cfstore.getStatusString());
                    bucket.setStatusTimestamp(String.valueOf(cfstore.getStatusTimestamp())
                            + "(" + new Date(cfstore.getStatusTimestamp()).toString() + ")");

                    JMXInstrumentedCacheMBean keyCacheMBean = probe.getKeyCacheMBean(tableName, cfstore.getColumnFamilyName());
                    if (keyCacheMBean.getCapacity() > 0)
                    {
                        bucket.setKeyCacheCapacity(keyCacheMBean.getCapacity());
                        bucket.setKeyCacheSize(keyCacheMBean.getSize());
                        bucket.setKeyCacheHitRate(keyCacheMBean.getRecentHitRate());
                    }
                    else
                    {
                        bucket.setKeyCacheCapacity(0);
                    }

                    JMXInstrumentedCacheMBean rowCacheMBean = probe.getRowCacheMBean(tableName, cfstore.getColumnFamilyName());
                    if (rowCacheMBean.getCapacity() > 0)
                    {
                        bucket.setRowCacheCapacity(rowCacheMBean.getCapacity());
                        bucket.setRowCacheSize(rowCacheMBean.getSize());
                        bucket.setRowCacheHitRate(rowCacheMBean.getRecentHitRate());
                    }
                    else
                    {
                        bucket.setRowCacheCapacity(0);
                    }

                    bucketList.add(bucket);
                }

                Result.SpaceInfo space = new Result.SpaceInfo();
                space.setName(tableName);
                space.setReadCount(tableReadCount);
                space.setReadLatency(tableReadLatency);
                space.setWriteCount(tableWriteCount);
                space.setWriteLatency(tableWriteLatency);
                space.setPendingTasks(tablePendingTasks);
                space.setBucketList(bucketList);
                spaceList.add(space);
            }
            
            Result.CFStats r = new Result.CFStats();
            r.buildOK();
            r.setSpaceList(spaceList);
            return mapper.writeValueAsString(r);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            logger.error("get node cfstate error : " + e);
            if (e instanceof IOException)
                NodeList.getInstance().setNodeDead(ip);

            Result.R r = new Result.R();
            r.buildError(e.toString());
            return mapper.writeValueAsString(r);
        }
    }
    
    private String procTotalSchema(String ip) throws JsonGenerationException, JsonMappingException, IOException
    {
        NodeProbe probe = NodeList.getInstance().getNodeProbe(ip);
        ObjectMapper mapper = new ObjectMapper();

        try
        {
            Map<String, List<ColumnFamilyStoreMBean>> cfstoreMap = new HashMap<String, List<ColumnFamilyStoreMBean>>();
            Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfamilies = probe
                    .getColumnFamilyStoreMBeanProxies();

            for (; cfamilies.hasNext();) 
            {
                Map.Entry<String, ColumnFamilyStoreMBean> entry = cfamilies
                        .next();
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
            
            List<KSMetaData> ksMetaDataList = probe.getSchema();

            List<Result.SpaceSchema> spaceSchemaList = new ArrayList<Result.SpaceSchema>();
            for (KSMetaData ksMetaData : ksMetaDataList)
            {
                if (ksMetaData.name.equals(Table.SYSTEM_TABLE)
                        || ksMetaData.name.equals(Collector.ClsSystem_KS))
                    continue;
                
                List<ColumnFamilyStoreMBean> columnFamilies = cfstoreMap.get(ksMetaData.name);
                int tableReadCount = 0;
                int tableWriteCount = 0;
                int tablePendingTasks = 0;
                double tableTotalReadTime = 0.0f;
                double tableTotalWriteTime = 0.0f;

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

                ArrayList<Result.BucketSchema> bucketSchemaList = new ArrayList<Result.BucketSchema>();
                for (CFMetaData cfMetaData : ksMetaData.cfMetaData.values())
                {
                    Result.BucketSchema bucket = new Result.BucketSchema();
                    bucket.setName(cfMetaData.cfName);
                    bucket.setComment(cfMetaData.comment);
                    bucket.setRowCacheSize(cfMetaData.rowCacheSize);
                    bucket.setKeyCacheSize(cfMetaData.keyCacheSize);
                    bucket.setCompactSkipSize(cfMetaData.compactSkipSize);
                    bucket
                            .setCompressAlgo(cfMetaData.compressAlgo == null ? null
                                    : cfMetaData.compressAlgo.getName());
                    bucketSchemaList.add(bucket);
                }
                
                Result.SpaceSchema space = new Result.SpaceSchema();
                
                space.setName(ksMetaData.name);
                
                space.setReadCount(tableReadCount);
                space.setReadLatency(tableReadLatency);
                space.setWriteCount(tableWriteCount);
                space.setWriteLatency(tableWriteLatency);
                space.setPendingTasks(tablePendingTasks);
                
                space.setReplicationFactor(ksMetaData.replicationFactor);
                space.setBucketSchemaList(bucketSchemaList);
                spaceSchemaList.add(space);
            }
            
            Result.TotalSchema r = new Result.TotalSchema();
            r.buildOK();
            r.setSpaceSchemaList(spaceSchemaList);
            return mapper.writeValueAsString(r);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            logger.error("get node total schema error : " + e);
            if (e instanceof IOException)
                NodeList.getInstance().setNodeDead(ip);

            Result.R r = new Result.R();
            r.buildError(e.toString());
            return mapper.writeValueAsString(r);
        }
    }
    
    private String procSingleBucketSchema(String ip, String space, String bucket) throws JsonGenerationException, JsonMappingException, IOException
    {
        NodeProbe probe = NodeList.getInstance().getNodeProbe(ip);
        ObjectMapper mapper = new ObjectMapper();
        
        try
        {
            KSMetaData ksMetaData = null;
            for (KSMetaData ks : probe.getSchema())
            {
                if (ks.name.equals(space))
                {
                    ksMetaData = ks;
                    CFMetaData cfMetaData = ksMetaData.cfMetaData.get(bucket);
                    if (cfMetaData == null)
                        throw new RuntimeException("can not find MetaData for "
                                + space + "." + bucket);
                    
                    Result.BucketSchema bs = new Result.BucketSchema();
 
                    bs.setName(cfMetaData.cfName);
                    bs.setComment(cfMetaData.comment);
                    bs.setRowCacheSize(cfMetaData.rowCacheSize);
                    bs.setKeyCacheSize(cfMetaData.keyCacheSize);
                    bs.setCompactSkipSize(cfMetaData.compactSkipSize);
                    bs.setCompressAlgo(cfMetaData.compressAlgo==null?null:cfMetaData.compressAlgo.getName());
                    
                    Result.BucketSchemaR r = new Result.BucketSchemaR();
                    r.buildOK();
                    r.setBucket(bs);
                    return mapper.writeValueAsString(r);
                }
            }
            if (ksMetaData == null)
                throw new RuntimeException("can not find MetaData for "+space);
            return null;
        }
        catch(Exception e)
        {
            e.printStackTrace();
            logger.error("get node single schema error : " + e);
            if (e instanceof IOException)
                NodeList.getInstance().setNodeDead(ip);

            Result.R r = new Result.R();
            r.buildError(e.toString());
            return mapper.writeValueAsString(r);
        }
    }

    
    private String procSingleBucketState(String ip, String space, String bucket) throws JsonGenerationException, JsonMappingException, IOException
    {
        NodeProbe probe = NodeList.getInstance().getNodeProbe(ip);
        ObjectMapper mapper = new ObjectMapper();
        
        try
        {
            Result.BucketInfo b = new Result.BucketInfo();
            
            ColumnFamilyStoreMBean cfstore = probe.getColumnFamilyMBean(space, bucket);
            
            b.setName(cfstore.getColumnFamilyName());
            b.setSsTableCount(cfstore.getLiveSSTableCount());
            b.setSpaceUsedLive(cfstore.getLiveDiskSpaceUsed());
            b.setSpaceUsedTotal(cfstore.getTotalDiskSpaceUsed());
            b.setMemtableCellCount(cfstore.getMemtableColumnsCount());
            b.setMemtableDataSize(cfstore.getMemtableDataSize());
            b.setMemtableSwitchCount(cfstore.getMemtableSwitchCount());
            b.setReadCount(cfstore.getReadCount());
            b.setReadLatency(cfstore.getRecentReadLatencyMicros()/1000);
            b.setReadThroughput(cfstore.getRecentReadThroughput());
            b.setWriteCount(cfstore.getWriteCount());
            b.setWriteLatency(cfstore.getRecentWriteLatencyMicros()/1000);
            b.setWriteThroughput(cfstore.getRecentWriteThroughput());
            b.setPendingTasks(cfstore.getPendingTasks());
            b.setCompactedRowMinimumSize(cfstore.getMinRowCompactedSize());
            b.setCompactedRowMaximumSize(cfstore.getMaxRowCompactedSize());
            b.setCompactedRowMeanSize(cfstore.getMeanRowCompactedSize());
            b.setStatus(cfstore.getStatusString());
            b.setStatusTimestamp(String.valueOf(cfstore.getStatusTimestamp())
                    + "(" + new Date(cfstore.getStatusTimestamp()).toString() + ")");
            
            JMXInstrumentedCacheMBean keyCacheMBean = probe.getKeyCacheMBean(space, bucket);
            if (keyCacheMBean.getCapacity() > 0)
            {
                b.setKeyCacheCapacity(keyCacheMBean.getCapacity());
                b.setKeyCacheSize(keyCacheMBean.getSize());
                b.setKeyCacheHitRate(keyCacheMBean.getRecentHitRate());
            }
            else
            {
                b.setKeyCacheCapacity(0);
            }

            JMXInstrumentedCacheMBean rowCacheMBean = probe.getRowCacheMBean(space, bucket);
            if (rowCacheMBean.getCapacity() > 0)
            {
                b.setRowCacheCapacity(rowCacheMBean.getCapacity());
                b.setRowCacheSize(rowCacheMBean.getSize());
                b.setRowCacheHitRate(rowCacheMBean.getRecentHitRate());
            }
            else
            {
                b.setRowCacheCapacity(0);
            }

            Result.BucketInfoR r = new Result.BucketInfoR();
            r.buildOK();
            r.setBucket(b);
            
            return mapper.writeValueAsString(r);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            logger.error("get node total schema error : " + e);
            if (e instanceof IOException)
                NodeList.getInstance().setNodeDead(ip);

            Result.R r = new Result.R();
            r.buildError(e.toString());
            return mapper.writeValueAsString(r);
        }
    }
    
    private String procProxyStats(String ip) throws JsonGenerationException, JsonMappingException, IOException
    {
        NodeProbe probe = NodeList.getInstance().getNodeProbe(ip);
        ObjectMapper mapper = new ObjectMapper();
        
        try
        {
            StorageProxyMBean spBean =probe.getStorageProxyMBean();
            Result.ProxyStats r = new Result.ProxyStats();
            r.buildOK();
            r.setReadOperations(spBean.getReadOperations());
            r.setTotalReadLatency(spBean.getTotalReadLatencyMicros() / 1000);
            r.setRecentReadLatency(spBean.getRecentReadLatencyMicros() / 1000);
            r.setRangeOperations(spBean.getRangeOperations());
            r.setTotalRangeLatency(spBean.getTotalRangeLatencyMicros() / 1000);
            r.setRecentRangeLatency(spBean.getRecentRangeLatencyMicros() / 1000);
            r.setWriteOperations(spBean.getWriteOperations());
            r.setTotalWriteLatency(spBean.getTotalWriteLatencyMicros() / 1000);
            r.setRecentWriteLatency(spBean.getRecentWriteLatencyMicros() / 1000);
            return mapper.writeValueAsString(r);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            logger.error("get proxy stats schema error : " + e);
            if (e instanceof IOException)
                NodeList.getInstance().setNodeDead(ip);

            Result.R r = new Result.R();
            r.buildError(e.toString());
            return mapper.writeValueAsString(r);
        }
    }
    
    private String procFSState(String ip) throws JsonGenerationException, JsonMappingException, IOException
    {
        NodeProbe probe = NodeList.getInstance().getNodeProbe(ip);
        ObjectMapper mapper = new ObjectMapper();
        
        try
        {
            Map<String, FileUtils.FSInfo> storageInfo = probe.getStorageFSInfo();
            Map<String, FileUtils.FSInfo> logInfo = probe.getLogFSInfo();

            List<Result.FSInfo> s = new ArrayList<Result.FSInfo>();
            List<Result.FSInfo> l = new ArrayList<Result.FSInfo>();
            
            for (Map.Entry<String, FileUtils.FSInfo> e : storageInfo.entrySet())
            {
                Result.FSInfo fs = new Result.FSInfo();
                fs.setFileName(e.getKey());
                fs.setFsName(e.getValue().fsName);
                fs.setMountOn(e.getValue().mountOn);
                fs.setTotal(FileUtils.stringifyFileSize(e.getValue().totalSize));
                fs.setUsed(FileUtils.stringifyFileSize(e.getValue().usedSize));
                s.add(fs);
            }

            for (Map.Entry<String, FileUtils.FSInfo> e : logInfo.entrySet())
            {
                Result.FSInfo fs = new Result.FSInfo();
                fs.setFileName(e.getKey());
                fs.setFsName(e.getValue().fsName);
                fs.setMountOn(e.getValue().mountOn);
                fs.setTotal(FileUtils.stringifyFileSize(e.getValue().totalSize));
                fs.setUsed(FileUtils.stringifyFileSize(e.getValue().usedSize));
                l.add(fs);
            }

            Result.TotalFSInfo r = new Result.TotalFSInfo();
            r.buildOK();
            r.setLogInfo(l);
            r.setStorageInfo(s);
            return mapper.writeValueAsString(r);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            logger.error("get fsstats error : " + e);
            if (e instanceof IOException)
                NodeList.getInstance().setNodeDead(ip);

            Result.R r = new Result.R();
            r.buildError(e.toString());
            return mapper.writeValueAsString(r);
        }
    }
    
    private String procBucketFlush(String ip, String space, String bucket) throws JsonGenerationException, JsonMappingException, IOException {
        NodeProbe probe = NodeList.getInstance().getNodeProbe(ip);
        ObjectMapper mapper = new ObjectMapper();
        
        try
        {
            probe.forceTableFlush(space, bucket);
            Result.R r = new Result.R();
            r.buildOK();
            return mapper.writeValueAsString(r);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            logger.error("oper flush error : " + e);
            if (e instanceof IOException)
                NodeList.getInstance().setNodeDead(ip);

            Result.R r = new Result.R();
            r.buildError(e.toString());
            return mapper.writeValueAsString(r);
        }
    }

    private String procBucketCompact(String ip, String space, String bucket) throws JsonGenerationException, JsonMappingException, IOException {
        NodeProbe probe = NodeList.getInstance().getNodeProbe(ip);
        ObjectMapper mapper = new ObjectMapper();

        try
        {
            probe.forceTableCompaction(1, space, bucket);
            Result.R r = new Result.R();
            r.buildOK();
            return mapper.writeValueAsString(r);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            logger.error("oper compact error : " + e);
            if (e instanceof IOException)
                NodeList.getInstance().setNodeDead(ip);

            Result.R r = new Result.R();
            r.buildError(e.toString());
            return mapper.writeValueAsString(r);
        }
    }

    private String procBucketReset(String ip, String space, String bucket) throws JsonGenerationException, JsonMappingException, IOException {
        NodeProbe probe = NodeList.getInstance().getNodeProbe(ip);
        ObjectMapper mapper = new ObjectMapper();

        try
        {
            probe.getStorageProxyMBean().graceResetClusterCF(space, bucket);
            Result.R r = new Result.R();
            r.buildOK();
            return mapper.writeValueAsString(r);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            logger.error("oper reset error : " + e);
            if (e instanceof IOException)
                NodeList.getInstance().setNodeDead(ip);

            Result.R r = new Result.R();
            r.buildError(e.toString());
            return mapper.writeValueAsString(r);
        }
    }

    private String procBucketUndoReset(String ip, String space, String bucket) throws JsonGenerationException, JsonMappingException, IOException {
        NodeProbe probe = NodeList.getInstance().getNodeProbe(ip);
        ObjectMapper mapper = new ObjectMapper();

        try
        {
            probe.getStorageProxyMBean().graceResetClusterCFUndo(space, bucket);
            Result.R r = new Result.R();
            r.buildOK();
            return mapper.writeValueAsString(r);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            logger.error("oper undo reset error : " + e);
            if (e instanceof IOException)
                NodeList.getInstance().setNodeDead(ip);

            Result.R r = new Result.R();
            r.buildError(e.toString());
            return mapper.writeValueAsString(r);
        }
    }
}
