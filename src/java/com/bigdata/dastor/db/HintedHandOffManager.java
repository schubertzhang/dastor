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

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutorService;
import java.io.IOException;

import org.apache.log4j.Logger;


import java.net.InetAddress;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.lang.ArrayUtils;


import com.bigdata.dastor.concurrent.JMXEnabledThreadPoolExecutor;
import com.bigdata.dastor.config.DatabaseDescriptor;
import com.bigdata.dastor.db.filter.QueryFilter;
import com.bigdata.dastor.db.filter.QueryPath;
import com.bigdata.dastor.db.filter.SliceQueryFilter;
import com.bigdata.dastor.gms.FailureDetector;
import com.bigdata.dastor.gms.Gossiper;
import com.bigdata.dastor.net.Message;
import com.bigdata.dastor.net.MessagingService;
import com.bigdata.dastor.service.*;
import com.bigdata.dastor.thrift.InvalidRequestException;
import com.bigdata.dastor.utils.WrappedRunnable;
import org.cliffc.high_scale_lib.NonBlockingHashSet;


/**
 * Following is the old 0.6 design:
 * For each table (keyspace), there is a row in the system hints CF.
 * SuperColumns in that row are keys for which we have hinted data.
 * Subcolumns names within that supercolumn are host IPs. Subcolumn values are always empty.
 * Instead, we store the row data "normally" in the application table it belongs in.
 *
 * So when we deliver hints we look up endpoints that need data delivered
 * on a per-key basis, then read that entire row out and send it over.
 * (TODO handle rows that have incrementally grown too large for a single message.)
 *
 * HHM never deletes the row from Application tables; there is no way to distinguish that
 * from hinted tombstones!  instead, rely on cleanup compactions to remove data
 * that doesn't belong on this node.  (Cleanup compactions may be started manually
 * -- on a per node basis -- with "nodeprobe cleanup.")
 *
 * TODO this avoids our hint rows from growing excessively large by offloading the
 * message data into application tables.  But, this means that cleanup compactions
 * will nuke HH data.  Probably better would be to store the RowMutation messages
 * in a HHData (non-super) CF, modifying the above to store a UUID value in the
 * HH subcolumn value, which we use as a key to a [standard] HHData system CF
 * that would contain the message bytes.
 *
 * There are two ways hinted data gets delivered to the intended nodes.
 *
 * runHints() runs periodically and pushes the hinted data on this node to
 * every intended node.
 *
 * runDelieverHints() is called when some other node starts up (potentially
 * from a failure) and delivers the hinted data just to that node.
 * 
 * =================================================================================
 * BIGDATA: Following is the new design:
 * =================================================================================
 * Schema of SystemTable.HintsColumnFamily:
 *  For each "endPoint-tableName-cfName" for which we have hints, there is a row in 
 *  the system hints CF.
 *  RowKey: "endPoint-tableName-cfName"
 *  Column: Columns in that row are keys for which we have hinted data in the 
 *  application table it belongs in.
 *
 * When FailureDetector signals that a node that was down is back up, we read its
 * hints rows from SystemTable.HintsColumnFamily to see what keys we need to 
 * forward data for, then reach each row in its entirety and send it over.
 *
 * deliverHints is also exposed to JMX so it can be run manually if FD ever misses
 * its cue somehow.
 *
 * HHM never deletes the row from Application tables; usually (but not for CL.ANY!)
 * the row belongs on this node, as well.  instead, we rely on cleanup compactions
 * to remove data that doesn't belong.  (Cleanup compactions may be started manually
 * -- on a per node basis -- with "nodeprobe cleanup.") 
 * 
 */

public class HintedHandOffManager implements HintedHandOffManagerMBean
{
    public static final HintedHandOffManager instance = new HintedHandOffManager();

    private static final Logger logger_ = Logger.getLogger(HintedHandOffManager.class);
    public static final String HINTS_CF = "HintsColumnFamily";
    private static final int PAGE_SIZE = 10000;
    private static final String SEPARATOR = "-";

    private final NonBlockingHashSet<InetAddress> queuedDeliveries = new NonBlockingHashSet<InetAddress>();

    private final ExecutorService executor_;

    // BIGDATA: ongoing delivering statistic for JMX
    public static final String MBEAN_OBJECT_NAME = "com.bigdata.dastor.db:type=DeputyTransfer";
    private long rowsReplayed = 0;
    private InetAddress deliveringEndPoint = null;
    private String deliveringTable = null;
    private String deliveringCf = null;
    
    // BIGDATA: add JMX
    static
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_OBJECT_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
    public HintedHandOffManager()
    {        
        int hhPriority = System.getProperty("bigdata.dastor.deputytransfer.priority") == null
                         ? Thread.NORM_PRIORITY
                         : Integer.parseInt(System.getProperty("bigdata.dastor.deputytransfer.priority"));
        executor_ = new JMXEnabledThreadPoolExecutor("HINTED-HANDOFF-POOL", hhPriority);
    }

    // BIGDATA: add cfName parameter, only for specified CF
    private static boolean sendMessage(InetAddress endPoint, String tableName, String cfName, String key) throws IOException
    {
        if (!Gossiper.instance.isKnownEndpoint(endPoint))
        {
            logger_.warn("Hinted handoff found for endpoint " + endPoint.getHostAddress() + " which is not part of the gossip network.  discarding.");
            return true;
        }
        if (!FailureDetector.instance.isAlive(endPoint))
        {
            logger_.warn("Hinted handoff found for endpoint " + endPoint.getHostAddress() + " which is not alive.  stopping.");
            return false;
        }

        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        byte[] startColumn = ArrayUtils.EMPTY_BYTE_ARRAY;
        while (true)
        {
            QueryFilter filter = new SliceQueryFilter(key, new QueryPath(cfs.getColumnFamilyName()), startColumn, ArrayUtils.EMPTY_BYTE_ARRAY, false, PAGE_SIZE);
            ColumnFamily cf = cfs.getColumnFamily(filter);
            if (pagingFinished(cf, startColumn))
                break;
            if (cf.getColumnNames().isEmpty())
            {
                if (logger_.isDebugEnabled())
                    logger_.debug("Nothing to hand off for " + key);
                break;
            }

            startColumn = cf.getColumnNames().last();
            RowMutation rm = new RowMutation(tableName, key);
            rm.add(cf);
            Message message = rm.makeRowMutationMessage();
            WriteResponseHandler responseHandler = new WriteResponseHandler(1, tableName);
            // BIGDATA: retry
            int tryNum = 0;
            for (tryNum = 0; tryNum < 3; tryNum++)
            {
                MessagingService.instance.sendRR(message, new InetAddress[] { endPoint }, responseHandler);
                try
                {
                    responseHandler.get();
                    break;
                }
                catch (TimeoutException e)
                {
                }
            }
            if (tryNum >= 3)
            {
                logger_.warn("Hinted handoff found for endpoint " + endPoint.getHostAddress() + " send message TimeoutException.");
                return false;
            }
        }
        return true;
    }

    /* BIGDATA: not used now, since hinted schema changed
    private static void deleteEndPoint(byte[] endpointAddress, String tableName, byte[] key, long timestamp) throws IOException
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, tableName);
        rm.delete(new QueryPath(HINTS_CF, key, endpointAddress), timestamp);
        rm.apply();
    }
    */

    private static void deleteHintKey(String hintKey, byte[] key) throws IOException
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, hintKey);
        rm.delete(new QueryPath(HINTS_CF, null, key), System.currentTimeMillis());
        rm.apply();
    }

    public static String makeHintKey(String endPoint,String tableName,String cfName)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(endPoint);
        sb.append(SEPARATOR);
        sb.append(tableName);
        sb.append(SEPARATOR);
        sb.append(cfName);
        return sb.toString();
    }

    private static boolean pagingFinished(ColumnFamily hintColumnFamily, byte[] startColumn)
    {
        // done if no hints found or the start column (same as last column processed in previous iteration) is the only one
        return hintColumnFamily == null
               || (hintColumnFamily.getSortedColumns().size() == 1 && hintColumnFamily.getColumn(startColumn) != null);
    }

    // BIGDATA: big changed since hinted schema changed, some back-port from issue-1142
    private void deliverHintsToEndpoint(InetAddress endPoint) throws IOException, DigestMismatchException, InvalidRequestException, TimeoutException
    {
        queuedDeliveries.remove(endPoint);

        // BIGDATA: sleep a little, because sometimes, the immediate sending will fail.
        try { Thread.sleep(10000); } catch (InterruptedException e) {}
        
        logger_.info("Hinted handoff START for endPoint " + endPoint.getHostAddress());
        long startTime = System.currentTimeMillis();

        // 1. For each table and cf, scan through SystemTable.HintsColumnFamily to find all rows 
        //    of this edpoint.
        // 2. For each key (column) send application data to the endpoint.
        // 3. Delete the key (column) from SystemTable.HintsColumnFamily.
        // 4. Now force a flush
        // 5. Do major compaction to clean up all deletes etc.
        rowsReplayed = 0; // BIGDATA for JMX: change int to long
        deliveringEndPoint = endPoint; // BIGDATA for JMX
        
        ColumnFamilyStore hintStore = Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(HINTS_CF);
        delivery:
        for (String tableName : DatabaseDescriptor.getNonSystemTables()) // BIGDATA: ignore the system table
        {
            deliveringTable = tableName; // BIGDATA for JMX            
            Set<String> cfs = Table.open(tableName).getColumnFamilies();
            for (String cf : cfs)
            {
                deliveringCf = cf;
                long rowsReplayedOfCf = 0;
                byte[] startColumn = ArrayUtils.EMPTY_BYTE_ARRAY;
                String hintKey = makeHintKey(endPoint.getHostAddress(), tableName, cf);
                while (true)
                {
                    QueryFilter filter = new SliceQueryFilter(hintKey, new QueryPath(HINTS_CF), startColumn, ArrayUtils.EMPTY_BYTE_ARRAY, false, PAGE_SIZE);
                    ColumnFamily hintColumnFamily = ColumnFamilyStore.removeDeleted(hintStore.getColumnFamily(filter), Integer.MAX_VALUE);
                    if (pagingFinished(hintColumnFamily, startColumn))
                        break;
                    Collection<IColumn> keys = hintColumnFamily.getSortedColumns();
                    
                    for (IColumn keyColumn : keys)
                    {
                        String keyStr = new String(keyColumn.name(), "UTF-8");

                        if (logger_.isDebugEnabled())
                            logger_.debug(String.format("Hinted handoff SENDING key %s to endpoint %s for kfsf %s:%s",
                                    keyStr, endPoint.getHostAddress(), tableName, cf));
                        
                        if (sendMessage(endPoint, tableName, cf, keyStr))
                        {
                            deleteHintKey(hintKey, keyColumn.name());
                            rowsReplayed++;
                            rowsReplayedOfCf++;
                        }
                        else
                        {
                            logger_.warn(String.format("Hinted handoff STOP, could not complete hinted handoff to %s when sending %s for kfsf %s:%s",
                                    endPoint.getHostAddress(), keyStr, tableName, cf));
                            // BIGDATA: here, the hinted data cannot be completely sent to the endPoint.
                            // It may because the endPoint is down again.
                            // the break will stop this hinted-handoff, and only another endpoint up/down can trigger another hinted-handoff.
                            break delivery;
                        }

                        startColumn = keyColumn.name();
                    }
                    
                    logger_.info(String.format("Hinted handoff PROGRESS, have sent %s/%s rows to %s for kfsf %s:%s",
                            rowsReplayedOfCf, rowsReplayed, endPoint.getHostAddress(), tableName, cf));
                }
                
                if (rowsReplayedOfCf > 0)
                {
                    logger_.info(String.format("Hinted handoff DONE of %s rows to endpoint %s for kscf %s:%s, used time(ms):%s",
                            rowsReplayedOfCf, endPoint.getHostAddress(), tableName, cf, System.currentTimeMillis()-startTime));
                }
            }
        }

        if (rowsReplayed > 0)
        {
            try
            {
                hintStore.forceBlockingFlush(); // BIGDATA: changed to blockingFlush
                CompactionManager.instance.submitMajor(hintStore, 0, Integer.MAX_VALUE).get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        logger_.info(String.format("Hinted handoff FINISHED of %s rows to endpoint %s, used time(ms):%s",
                                   rowsReplayed, endPoint.getHostAddress(), System.currentTimeMillis()-startTime));
        // BIGDATA: clean JMX
        rowsReplayed = 0;
        deliveringEndPoint = null;
        deliveringTable = null;
        deliveringCf = null;
    }

    /*
     * This method is used to deliver hints to a particular endpoint.
     * When we learn that some endpoint is back up we deliver the data
     * to him via an event driven mechanism.
    */
    public void deliverHints(final InetAddress to)
    {
        if (!queuedDeliveries.add(to))
            return;

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                deliverHintsToEndpoint(to);
            }
        };
    	executor_.submit(r);
    }

    @Override
    public void deliverHints(String to) throws UnknownHostException
    {
        deliverHints(InetAddress.getByName(to));
    }
    
    // BIGDATA
    @Override
    public long getDeliveredRows()
    {
        return rowsReplayed;
    }
    
    // BIGDATA
    @Override
    public String getDeliveringEp()
    {
        if (deliveringEndPoint != null)
        {
            return deliveringEndPoint.getHostAddress();
        }
        return null;
    }
    
    // BIGDATA
    @Override
    public String getDeliveringTable()
    {
        return deliveringTable;
    }
    
    // BIGDATA
    @Override
    public String getDeliveringCf()
    {
        return deliveringCf;
    }
}
