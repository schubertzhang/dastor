This is a modified version of [Apache Cassandra](http://cassandra.apache.org/), based on a early version 0.6.8 (2011). It tried to make Cassandra more flexible and more available.  
Please forgive me that it is renamed as "dastor" and the java packages are renamed as "com.bigdata.dastor...". There is no disrespect to it's mother Cassandra.  

But finally, we think the architecture and design-principle from Cassandra/[Dynamo](http://www.allthingsdistributed.com/2007/10/amazons_dynamo.html) is wrong for big-data storage and then give it up. Refer to this article: [深入研究Cassandra后重读Dynamo Paper](http://www.slideshare.net/schubertzhang/cassandra-dynamo-paper) -- it is a chinese article. And in fact, we finally prefer [Google Bigtable](http://research.google.com/archive/bigtable.html) and [Apache HBase](http://hbase.apache.org/).  

Please refers to following material for some detail.  
[DaStor/Cassandra Evaluation Report for CDR Storage & Query](http://www.slideshare.net/schubertzhang/dastorcassandra-report-for-cdr-solution)  
[Cassandra Compression](http://www.slideshare.net/schubertzhang/cassandra-performanceevaluationwithcompression)   


# Developed Features  

## Admin Tools  
 - Configuration improvement based on config-files  
 - Script framework and scripts  
 - Admin tools  
 - CLI shell  
 - WebAdmin  
 - Ganglia, Jmxetric  
 
## Compression  
 - New serialization format.  
 - Support Gzip and LZO.  

## Bucket mapping and reclaim  
 - Mapping plug-in  
 - Reclaim command and mechanism.  

## Java Client API  
 - Easy and Simple  

## Concurrent Compaction  
 - From single thread to bucket- independent multi-threads.  

## Scalability  
 - Easy to scale-out  
 - More controllable  

## Benchmarks  
 - Writes and Reads  
 - Throughput and Latency  

## Bug fix  
 - ...  

-----------------------------------------------------------------------------

# DaStor/Cassandra vs. Bigtable  

**Scalability:** Bigtable has better scalability.  
The scale of DaStor/Cassandra should be controlled carefully, and may affect services. It is a big trouble.  
Bigtable's scalability is easy.  

**Data Distribution:** Bigtable’s high-level partitioning/indexing scheme is more fine-grained, and so more effective.  
DaStor/Cassandra's consistent hash partitioning scheme is too coarse-grained, and so we must cut up the bucket level partitions. But sometimes, it is not easy to trade-off on bigdata.  

**Indexing:** Bigtable may need less memory to hold indexes.  
Bigtable's indexes are more general and can be shared equally (均摊) by different users/rows, especially when data-skew.   
There’s only one copy of indexes in Bigtable, even for multiple storage replications, since Bigtable use GFS layer for replication. (multiple copies of data, one copy of indexes)  

**Local Storage Engine:** Bigtable provides better read performance, less disk seeks.  
Bigtable vs. Cassandra ? InnoDB vs. MyISAM   

**In my opinion, Bigtable ’s architecture and data model make more sense.**    
The Cassandra project maybe a fault for big-data, and maybe a big fault to mix Dynamo and Bigtable. Cassandra is just a partial Dynamo and target to a wrong field - Big Data Storage. It is anamorphotic.  
