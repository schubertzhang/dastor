package com.bigdata.dastor.web;

import java.util.List;

import org.codehaus.jackson.annotate.JsonAutoDetect;

public class Result {

    @JsonAutoDetect 
    public static class R{
        String result;
        String reason;
        /**
         * @return the result
         */
        public String getResult() {
            return result;
        }
        /**
         * @param result the result to set
         */
        public void setResult(String result) {
            this.result = result;
        }
        /**
         * @return the reason
         */
        public String getReason() {
            return reason;
        }
        /**
         * @param reason the reason to set
         */
        public void setReason(String reason) {
            this.reason = reason;
        }
        void buildOK(){
            this.result = "ok";
            this.reason = "ok";
        }
        void buildError(){
            this.result = "error";
            this.reason = "error";
        }
        void buildError(String reason){
            this.result = "error";
            this.reason = reason;
        }
    }
    
    @JsonAutoDetect 
    static class NodeInfo{
        String ip;
        boolean alive;
        String operating;
        int generation;
        String uptime;
        String configuredCapacity;
        String load;
        String grossLoad;
        String heapusage;
        /**
         * @return the ip
         */
        public String getIp() {
            return ip;
        }
        /**
         * @param ip the ip to set
         */
        public void setIp(String ip) {
            this.ip = ip;
        }
        /**
         * @return the alive
         */
        public boolean isAlive() {
            return alive;
        }
        /**
         * @param alive the alive to set
         */
        public void setAlive(boolean alive) {
            this.alive = alive;
        }
        /**
         * @return the operating
         */
        public String getOperating() {
            return operating;
        }
        /**
         * @param operating the operating to set
         */
        public void setOperating(String operating) {
            this.operating = operating;
        }
        /**
         * @return the generation
         */
        public int getGeneration() {
            return generation;
        }
        /**
         * @param generation the generation to set
         */
        public void setGeneration(int generation) {
            this.generation = generation;
        }
        /**
         * @return the uptime
         */
        public String getUptime() {
            return uptime;
        }
        /**
         * @param uptime the uptime to set
         */
        public void setUptime(String uptime) {
            this.uptime = uptime;
        }
        /**
         * @return the configuredCapacity
         */
        public String getConfiguredCapacity() {
            return configuredCapacity;
        }
        /**
         * @param configuredCapacity the configuredCapacity to set
         */
        public void setConfiguredCapacity(String configuredCapacity) {
            this.configuredCapacity = configuredCapacity;
        }
        /**
         * @return the load
         */
        public String getLoad() {
            return load;
        }
        /**
         * @param load the load to set
         */
        public void setLoad(String load) {
            this.load = load;
        }
        /**
         * @return the grossLoad
         */
        public String getGrossLoad() {
            return grossLoad;
        }
        /**
         * @param grossLoad the grossLoad to set
         */
        public void setGrossLoad(String grossLoad) {
            this.grossLoad = grossLoad;
        }
        /**
         * @return the heapusage
         */
        public String getHeapusage() {
            return heapusage;
        }
        /**
         * @param heapusage the heapusage to set
         */
        public void setHeapusage(String heapusage) {
            this.heapusage = heapusage;
        }
    }

    static class ClusterName extends R{
        String name;
        /**
         * @return the name
         */
        public String getName() {
            return name;
        }
        /**
         * @param name the name to set
         */
        public void setName(String name) {
            this.name = name;
        }
    }
    
    @JsonAutoDetect 
    static class NodeInfoList extends R{
        List<NodeInfo> liveList;
        List<NodeInfo> deadList;
        /**
         * @return the liveList
         */
        public List<NodeInfo> getLiveList() {
            return liveList;
        }
        /**
         * @param liveList the liveList to set
         */
        public void setLiveList(List<NodeInfo> liveList) {
            this.liveList = liveList;
        }
        /**
         * @return the deadList
         */
        public List<NodeInfo> getDeadList() {
            return deadList;
        }
        /**
         * @param deadList the deadList to set
         */
        public void setDeadList(List<NodeInfo> deadList) {
            this.deadList = deadList;
        }
    }
    
    @JsonAutoDetect
    static class NodeInfoR extends R{
        NodeInfo node;
        /**
         * @return the node
         */
        public NodeInfo getNode() {
            return node;
        }
        /**
         * @param node the node to set
         */
        public void setNode(NodeInfo node) {
            this.node = node;
        }
    }

    @JsonAutoDetect
    static class BucketInfo{
        String name;
        int ssTableCount;
        long spaceUsedLive;
        long spaceUsedTotal;
        int memtableCellCount;
        int memtableDataSize;
        int memtableSwitchCount;
        long readCount;
        double readLatency;
        double readThroughput;
        long writeCount;
        double writeLatency;
        double writeThroughput;
        int pendingTasks;
        int keyCacheCapacity;
        int keyCacheSize;
        double keyCacheHitRate;
        int rowCacheCapacity;
        int rowCacheSize;
        double rowCacheHitRate;
        long compactedRowMinimumSize;
        long compactedRowMaximumSize;
        long compactedRowMeanSize;
        String status;
        String statusTimestamp;
        /**
         * @return the name
         */
        public String getName() {
            return name;
        }
        /**
         * @param name the name to set
         */
        public void setName(String name) {
            this.name = name;
        }
        /**
         * @return the ssTableCount
         */
        public int getSsTableCount() {
            return ssTableCount;
        }
        /**
         * @param ssTableCount the ssTableCount to set
         */
        public void setSsTableCount(int ssTableCount) {
            this.ssTableCount = ssTableCount;
        }
        /**
         * @return the spaceUsedLive
         */
        public long getSpaceUsedLive() {
            return spaceUsedLive;
        }
        /**
         * @param spaceUsedLive the spaceUsedLive to set
         */
        public void setSpaceUsedLive(long spaceUsedLive) {
            this.spaceUsedLive = spaceUsedLive;
        }
        /**
         * @return the spaceUsedTotal
         */
        public long getSpaceUsedTotal() {
            return spaceUsedTotal;
        }
        /**
         * @param spaceUsedTotal the spaceUsedTotal to set
         */
        public void setSpaceUsedTotal(long spaceUsedTotal) {
            this.spaceUsedTotal = spaceUsedTotal;
        }
        /**
         * @return the memtableColumnCount
         */
        public int getMemtableCellCount() {
            return memtableCellCount;
        }
        /**
         * @param memtableColumnCount the memtableColumnCount to set
         */
        public void setMemtableCellCount(int memtableCellCount) {
            this.memtableCellCount = memtableCellCount;
        }
        /**
         * @return the memtableDataSize
         */
        public int getMemtableDataSize() {
            return memtableDataSize;
        }
        /**
         * @param memtableDataSize the memtableDataSize to set
         */
        public void setMemtableDataSize(int memtableDataSize) {
            this.memtableDataSize = memtableDataSize;
        }
        /**
         * @return the memtableSwitchCount
         */
        public int getMemtableSwitchCount() {
            return memtableSwitchCount;
        }
        /**
         * @param memtableSwitchCount the memtableSwitchCount to set
         */
        public void setMemtableSwitchCount(int memtableSwitchCount) {
            this.memtableSwitchCount = memtableSwitchCount;
        }
        /**
         * @return the readCount
         */
        public long getReadCount() {
            return readCount;
        }
        /**
         * @param readCount the readCount to set
         */
        public void setReadCount(long readCount) {
            this.readCount = readCount;
        }
        /**
         * @return the readLatency
         */
        public double getReadLatency() {
            return readLatency;
        }
        /**
         * @param readLatency the readLatency to set
         */
        public void setReadLatency(double readLatency) {
            this.readLatency = readLatency;
        }
        /**
         * @return the readThroughput
         */
        public double getReadThroughput() {
            return readThroughput;
        }
        /**
         * @param readThroughput the readThroughput to set
         */
        public void setReadThroughput(double readThroughput) {
            this.readThroughput = readThroughput;
        }
        /**
         * @return the writeCount
         */
        public long getWriteCount() {
            return writeCount;
        }
        /**
         * @param writeCount the writeCount to set
         */
        public void setWriteCount(long writeCount) {
            this.writeCount = writeCount;
        }
        /**
         * @return the writeLatency
         */
        public double getWriteLatency() {
            return writeLatency;
        }
        /**
         * @param writeLatency the writeLatency to set
         */
        public void setWriteLatency(double writeLatency) {
            this.writeLatency = writeLatency;
        }
        /**
         * @return the writeThroughput
         */
        public double getWriteThroughput() {
            return writeThroughput;
        }
        /**
         * @param writeThroughput the writeThroughput to set
         */
        public void setWriteThroughput(double writeThroughput) {
            this.writeThroughput = writeThroughput;
        }
        /**
         * @return the pendingTasks
         */
        public int getPendingTasks() {
            return pendingTasks;
        }
        /**
         * @param pendingTasks the pendingTasks to set
         */
        public void setPendingTasks(int pendingTasks) {
            this.pendingTasks = pendingTasks;
        }
        /**
         * @return the keyCacheCapacity
         */
        public int getKeyCacheCapacity() {
            return keyCacheCapacity;
        }
        /**
         * @param keyCacheCapacity the keyCacheCapacity to set
         */
        public void setKeyCacheCapacity(int keyCacheCapacity) {
            this.keyCacheCapacity = keyCacheCapacity;
        }
        /**
         * @return the keyCacheSize
         */
        public int getKeyCacheSize() {
            return keyCacheSize;
        }
        /**
         * @param keyCacheSize the keyCacheSize to set
         */
        public void setKeyCacheSize(int keyCacheSize) {
            this.keyCacheSize = keyCacheSize;
        }
        /**
         * @return the keyCacheHitRate
         */
        public double getKeyCacheHitRate() {
            return keyCacheHitRate;
        }
        /**
         * @param keyCacheHitRate the keyCacheHitRate to set
         */
        public void setKeyCacheHitRate(double keyCacheHitRate) {
            this.keyCacheHitRate = keyCacheHitRate;
        }
        /**
         * @return the rowCacheCapacity
         */
        public int getRowCacheCapacity() {
            return rowCacheCapacity;
        }
        /**
         * @param rowCacheCapacity the rowCacheCapacity to set
         */
        public void setRowCacheCapacity(int rowCacheCapacity) {
            rowCacheCapacity = rowCacheCapacity;
        }
        /**
         * @return the rowCacheSize
         */
        public int getRowCacheSize() {
            return rowCacheSize;
        }
        /**
         * @param rowCacheSize the rowCacheSize to set
         */
        public void setRowCacheSize(int rowCacheSize) {
            rowCacheSize = rowCacheSize;
        }
        /**
         * @return the rowCacheHitRate
         */
        public double getRowCacheHitRate() {
            return rowCacheHitRate;
        }
        /**
         * @param rowCacheHitRate the rowCacheHitRate to set
         */
        public void setRowCacheHitRate(double rowCacheHitRate) {
            rowCacheHitRate = rowCacheHitRate;
        }
        /**
         * @return the compactedRowMinimumSize
         */
        public long getCompactedRowMinimumSize() {
            return compactedRowMinimumSize;
        }
        /**
         * @param compactedRowMinimumSize the compactedRowMinimumSize to set
         */
        public void setCompactedRowMinimumSize(long compactedRowMinimumSize) {
            this.compactedRowMinimumSize = compactedRowMinimumSize;
        }
        /**
         * @return the compactedRowMaximumSize
         */
        public long getCompactedRowMaximumSize() {
            return compactedRowMaximumSize;
        }
        /**
         * @param compactedRowMaximumSize the compactedRowMaximumSize to set
         */
        public void setCompactedRowMaximumSize(long compactedRowMaximumSize) {
            this.compactedRowMaximumSize = compactedRowMaximumSize;
        }
        /**
         * @return the compactedRowMeanSize
         */
        public long getCompactedRowMeanSize() {
            return compactedRowMeanSize;
        }
        /**
         * @param compactedRowMeanSize the compactedRowMeanSize to set
         */
        public void setCompactedRowMeanSize(long compactedRowMeanSize) {
            this.compactedRowMeanSize = compactedRowMeanSize;
        }
        /**
         * @return the status
         */
        public String getStatus() {
            return status;
        }
        /**
         * @param status the status to set
         */
        public void setStatus(String status) {
            this.status = status;
        }
        /**
         * @return the statusTimestamp
         */
        public String getStatusTimestamp() {
            return statusTimestamp;
        }
        /**
         * @param statusTimestamp the statusTimestamp to set
         */
        public void setStatusTimestamp(String statusTimestamp) {
            this.statusTimestamp = statusTimestamp;
        }
    }
    
    @JsonAutoDetect
    static class BucketInfoR extends R{
        BucketInfo bucket;
        /**
         * @return the bucket
         */
        public BucketInfo getBucket() {
            return bucket;
        }
        /**
         * @param bucket the bucket to set
         */
        public void setBucket(BucketInfo bucket) {
            this.bucket = bucket;
        }
    }
    
    @JsonAutoDetect
    static class SpaceInfo{
        String name;
        int readCount;
        double readLatency;
        int writeCount;
        double writeLatency;
        int pendingTasks;
        List<BucketInfo> bucketList;
        /**
         * @return the name
         */
        public String getName() {
            return name;
        }
        /**
         * @param name the name to set
         */
        public void setName(String name) {
            this.name = name;
        }
        /**
         * @return the readCount
         */
        public int getReadCount() {
            return readCount;
        }
        /**
         * @param readCount the readCount to set
         */
        public void setReadCount(int readCount) {
            this.readCount = readCount;
        }
        /**
         * @return the readLatency
         */
        public double getReadLatency() {
            return readLatency;
        }
        /**
         * @param readLatency the readLatency to set
         */
        public void setReadLatency(double readLatency) {
            this.readLatency = readLatency;
        }
        /**
         * @return the writeCount
         */
        public int getWriteCount() {
            return writeCount;
        }
        /**
         * @param writeCount the writeCount to set
         */
        public void setWriteCount(int writeCount) {
            this.writeCount = writeCount;
        }
        /**
         * @return the writeLatency
         */
        public double getWriteLatency() {
            return writeLatency;
        }
        /**
         * @param writeLatency the writeLatency to set
         */
        public void setWriteLatency(double writeLatency) {
            this.writeLatency = writeLatency;
        }
        /**
         * @return the pendingTasks
         */
        public int getPendingTasks() {
            return pendingTasks;
        }
        /**
         * @param pendingTasks the pendingTasks to set
         */
        public void setPendingTasks(int pendingTasks) {
            this.pendingTasks = pendingTasks;
        }
        /**
         * @return the bucketList
         */
        public List<BucketInfo> getBucketList() {
            return bucketList;
        }
        /**
         * @param bucketList the bucketList to set
         */
        public void setBucketList(List<BucketInfo> bucketList) {
            this.bucketList = bucketList;
        }
    }
    
    @JsonAutoDetect
    static class CFStats extends R{
        List<SpaceInfo> spaceList;
        /**
         * @return the spaceList
         */
        public List<SpaceInfo> getSpaceList() {
            return spaceList;
        }
        /**
         * @param spaceList the spaceList to set
         */
        public void setSpaceList(List<SpaceInfo> spaceList) {
            this.spaceList = spaceList;
        }
    }
    
    @JsonAutoDetect
    static class BucketSchema{
        String name;
        String comment;
        double rowCacheSize;
        double keyCacheSize;
        long compactSkipSize;
        String compressAlgo;
        /**
         * @return the name
         */
        public String getName() {
            return name;
        }
        /**
         * @param name the name to set
         */
        public void setName(String name) {
            this.name = name;
        }
        /**
         * @return the comment
         */
        public String getComment() {
            return comment;
        }
        /**
         * @param comment the comment to set
         */
        public void setComment(String comment) {
            this.comment = comment;
        }
        /**
         * @return the rowCacheSize
         */
        public double getRowCacheSize() {
            return rowCacheSize;
        }
        /**
         * @param rowCacheSize the rowCacheSize to set
         */
        public void setRowCacheSize(double rowCacheSize) {
            this.rowCacheSize = rowCacheSize;
        }
        /**
         * @return the keyCacheSize
         */
        public double getKeyCacheSize() {
            return keyCacheSize;
        }
        /**
         * @param keyCacheSize the keyCacheSize to set
         */
        public void setKeyCacheSize(double keyCacheSize) {
            this.keyCacheSize = keyCacheSize;
        }
        /**
         * @return the compactSkipSize
         */
        public long getCompactSkipSize() {
            return compactSkipSize;
        }
        /**
         * @param compactSkipSize the compactSkipSize to set
         */
        public void setCompactSkipSize(long compactSkipSize) {
            this.compactSkipSize = compactSkipSize;
        }
        /**
         * @return the compressAlgo
         */
        public String getCompressAlgo() {
            return compressAlgo;
        }
        /**
         * @param compressAlgo the compressAlgo to set
         */
        public void setCompressAlgo(String compressAlgo) {
            this.compressAlgo = compressAlgo;
        }
    }
    
    @JsonAutoDetect
    static class BucketSchemaR extends R{
        BucketSchema bucket;
        /**
         * @return the bucket
         */
        public BucketSchema getBucket() {
            return bucket;
        }
        /**
         * @param bucket the bucket to set
         */
        public void setBucket(BucketSchema bucket) {
            this.bucket = bucket;
        }
    }
    
    @JsonAutoDetect
    static class SpaceSchema{
        String name;
        int readCount;
        double readLatency;
        int writeCount;
        double writeLatency;
        int pendingTasks;
        int replicationFactor;
        List<BucketSchema> bucketSchemaList;
        /**
         * @return the name
         */
        public String getName() {
            return name;
        }
        /**
         * @param name the name to set
         */
        public void setName(String name) {
            this.name = name;
        }
        /**
         * @return the readCount
         */
        public int getReadCount() {
            return readCount;
        }
        /**
         * @param readCount the readCount to set
         */
        public void setReadCount(int readCount) {
            this.readCount = readCount;
        }
        /**
         * @return the readLatency
         */
        public double getReadLatency() {
            return readLatency;
        }
        /**
         * @param readLatency the readLatency to set
         */
        public void setReadLatency(double readLatency) {
            this.readLatency = readLatency;
        }
        /**
         * @return the writeCount
         */
        public int getWriteCount() {
            return writeCount;
        }
        /**
         * @param writeCount the writeCount to set
         */
        public void setWriteCount(int writeCount) {
            this.writeCount = writeCount;
        }
        /**
         * @return the writeLatency
         */
        public double getWriteLatency() {
            return writeLatency;
        }
        /**
         * @param writeLatency the writeLatency to set
         */
        public void setWriteLatency(double writeLatency) {
            this.writeLatency = writeLatency;
        }
        /**
         * @return the pendingTasks
         */
        public int getPendingTasks() {
            return pendingTasks;
        }
        /**
         * @param pendingTasks the pendingTasks to set
         */
        public void setPendingTasks(int pendingTasks) {
            this.pendingTasks = pendingTasks;
        }
        /**
         * @return the replicationFactor
         */
        public int getReplicationFactor() {
            return replicationFactor;
        }
        /**
         * @param replicationFactor the replicationFactor to set
         */
        public void setReplicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
        }
        /**
         * @return the bucketSchemaList
         */
        public List<BucketSchema> getBucketSchemaList() {
            return bucketSchemaList;
        }
        /**
         * @param bucketSchemaList the bucketSchemaList to set
         */
        public void setBucketSchemaList(List<BucketSchema> bucketSchemaList) {
            this.bucketSchemaList = bucketSchemaList;
        }
    }
    
    @JsonAutoDetect
    static class TotalSchema extends R{
        List<SpaceSchema> spaceSchemaList;
        /**
         * @return the spaceSchemaList
         */
        public List<SpaceSchema> getSpaceSchemaList() {
            return spaceSchemaList;
        }
        /**
         * @param spaceSchemaList the spaceSchemaList to set
         */
        public void setSpaceSchemaList(List<SpaceSchema> spaceSchemaList) {
            this.spaceSchemaList = spaceSchemaList;
        }
    }
    
    @JsonAutoDetect
    static class ProxyStats extends R{
        long readOperations;
        long totalReadLatency;
        double recentReadLatency;
        long rangeOperations;
        long totalRangeLatency;
        double recentRangeLatency;
        long writeOperations;
        long totalWriteLatency;
        double recentWriteLatency;
        /**
         * @return the readOperations
         */
        public long getReadOperations() {
            return readOperations;
        }
        /**
         * @param readOperations the readOperations to set
         */
        public void setReadOperations(long readOperations) {
            this.readOperations = readOperations;
        }
        /**
         * @return the totalReadLatency
         */
        public long getTotalReadLatency() {
            return totalReadLatency;
        }
        /**
         * @param totalReadLatency the totalReadLatency to set
         */
        public void setTotalReadLatency(long totalReadLatency) {
            this.totalReadLatency = totalReadLatency;
        }
        /**
         * @return the recentReadLatency
         */
        public double getRecentReadLatency() {
            return recentReadLatency;
        }
        /**
         * @param recentReadLatency the recentReadLatency to set
         */
        public void setRecentReadLatency(double recentReadLatency) {
            this.recentReadLatency = recentReadLatency;
        }
        /**
         * @return the rangeOperations
         */
        public long getRangeOperations() {
            return rangeOperations;
        }
        /**
         * @param rangeOperations the rangeOperations to set
         */
        public void setRangeOperations(long rangeOperations) {
            this.rangeOperations = rangeOperations;
        }
        /**
         * @return the totalRangeLatency
         */
        public long getTotalRangeLatency() {
            return totalRangeLatency;
        }
        /**
         * @param totalRangeLatency the totalRangeLatency to set
         */
        public void setTotalRangeLatency(long totalRangeLatency) {
            this.totalRangeLatency = totalRangeLatency;
        }
        /**
         * @return the recentRangeLatency
         */
        public double getRecentRangeLatency() {
            return recentRangeLatency;
        }
        /**
         * @param recentRangeLatency the recentRangeLatency to set
         */
        public void setRecentRangeLatency(double recentRangeLatency) {
            this.recentRangeLatency = recentRangeLatency;
        }
        /**
         * @return the writeOperations
         */
        public long getWriteOperations() {
            return writeOperations;
        }
        /**
         * @param writeOperations the writeOperations to set
         */
        public void setWriteOperations(long writeOperations) {
            this.writeOperations = writeOperations;
        }
        /**
         * @return the totalWriteLatency
         */
        public long getTotalWriteLatency() {
            return totalWriteLatency;
        }
        /**
         * @param totalWriteLatency the totalWriteLatency to set
         */
        public void setTotalWriteLatency(long totalWriteLatency) {
            this.totalWriteLatency = totalWriteLatency;
        }
        /**
         * @return the recentWriteLatency
         */
        public double getRecentWriteLatency() {
            return recentWriteLatency;
        }
        /**
         * @param recentWriteLatency the recentWriteLatency to set
         */
        public void setRecentWriteLatency(double recentWriteLatency) {
            this.recentWriteLatency = recentWriteLatency;
        }
    }
    
    @JsonAutoDetect
    static class FSInfo{
        String fileName;
        String fsName;
        String mountOn;
        String total;
        String used;
        /**
         * @return the fileName
         */
        public String getFileName() {
            return fileName;
        }
        /**
         * @param fileName the fileName to set
         */
        public void setFileName(String fileName) {
            this.fileName = fileName;
        }
        /**
         * @return the fsName
         */
        public String getFsName() {
            return fsName;
        }
        /**
         * @param fsName the fsName to set
         */
        public void setFsName(String fsName) {
            this.fsName = fsName;
        }
        /**
         * @return the mountOn
         */
        public String getMountOn() {
            return mountOn;
        }
        /**
         * @param mountOn the mountOn to set
         */
        public void setMountOn(String mountOn) {
            this.mountOn = mountOn;
        }
        /**
         * @return the total
         */
        public String getTotal() {
            return total;
        }
        /**
         * @param total the total to set
         */
        public void setTotal(String total) {
            this.total = total;
        }
        /**
         * @return the used
         */
        public String getUsed() {
            return used;
        }
        /**
         * @param used the used to set
         */
        public void setUsed(String used) {
            this.used = used;
        }
    }
    
    @JsonAutoDetect
    static class TotalFSInfo extends R{
        List<FSInfo> logInfo;
        List<FSInfo> storageInfo;
        /**
         * @return the logInfo
         */
        public List<FSInfo> getLogInfo() {
            return logInfo;
        }
        /**
         * @param logInfo the logInfo to set
         */
        public void setLogInfo(List<FSInfo> logInfo) {
            this.logInfo = logInfo;
        }
        /**
         * @return the storageInfo
         */
        public List<FSInfo> getStorageInfo() {
            return storageInfo;
        }
        /**
         * @param storageInfo the storageInfo to set
         */
        public void setStorageInfo(List<FSInfo> storageInfo) {
            this.storageInfo = storageInfo;
        }
    }
}
