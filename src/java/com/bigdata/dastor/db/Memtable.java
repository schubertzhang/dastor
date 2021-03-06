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
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import org.apache.log4j.Logger;
import org.apache.commons.lang.ArrayUtils;

import com.bigdata.dastor.config.DatabaseDescriptor;
import com.bigdata.dastor.db.filter.*;
import com.bigdata.dastor.db.marshal.AbstractType;
import com.bigdata.dastor.dht.IPartitioner;
import com.bigdata.dastor.io.SSTableReader;
import com.bigdata.dastor.io.SSTableWriter;
import com.bigdata.dastor.io.util.DataOutputBuffer;
import com.bigdata.dastor.service.StorageService;
import com.bigdata.dastor.utils.WrappedRunnable;

public class Memtable implements Comparable<Memtable>, IFlushable
{
    private static final Logger logger = Logger.getLogger(Memtable.class);

    private boolean isFrozen;

    private final int THRESHOLD = DatabaseDescriptor.getMemtableThroughput() * 1024*1024; // not static since we might want to change at runtime
    private final int THRESHOLD_COUNT = (int)(DatabaseDescriptor.getMemtableOperations() * 1024*1024);

    private final AtomicInteger currentThroughput = new AtomicInteger(0);
    private final AtomicInteger currentOperations = new AtomicInteger(0);

    private final long creationTime;
    private final ConcurrentNavigableMap<DecoratedKey, ColumnFamily> columnFamilies = new ConcurrentSkipListMap<DecoratedKey, ColumnFamily>();
    private final IPartitioner partitioner = StorageService.getPartitioner();
    private final ColumnFamilyStore cfs;

    public Memtable(ColumnFamilyStore cfs)
    {

        this.cfs = cfs;
        creationTime = System.currentTimeMillis();
    }

    /**
     * Compares two Memtable based on creation time.
     * @param rhs Memtable to compare to.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     */
    public int compareTo(Memtable rhs)
    {
    	long diff = creationTime - rhs.creationTime;
    	if ( diff > 0 )
    		return 1;
    	else if ( diff < 0 )
    		return -1;
    	else
    		return 0;
    }

    public int getCurrentThroughput()
    {
        return currentThroughput.get();
    }
    
    public int getCurrentOperations()
    {
        return currentOperations.get();
    }

    boolean isThresholdViolated()
    {
        return currentThroughput.get() >= this.THRESHOLD || currentOperations.get() >= this.THRESHOLD_COUNT;
    }

    boolean isFrozen()
    {
        return isFrozen;
    }

    void freeze()
    {
        isFrozen = true;
    }

    /**
     * Should only be called by ColumnFamilyStore.apply.  NOT a public API.
     * (CFS handles locking to avoid submitting an op
     *  to a flushing memtable.  Any other way is unsafe.)
    */
    void put(String key, ColumnFamily columnFamily)
    {
        assert !isFrozen; // not 100% foolproof but hell, it's an assert
        resolve(key, columnFamily);
    }

    private void resolve(String key, ColumnFamily cf)
    {
        currentThroughput.addAndGet(cf.size());
        currentOperations.addAndGet(cf.getColumnCount());

        DecoratedKey decoratedKey = partitioner.decorateKey(key);
        ColumnFamily oldCf = columnFamilies.putIfAbsent(decoratedKey, cf);
        if (oldCf == null)
            return;

        oldCf.resolve(cf);
    }

    // for debugging
    public String contents()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : columnFamilies.entrySet())
        {
            builder.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        builder.append("}");
        return builder.toString();
    }


    private SSTableReader writeSortedContents() throws IOException
    {
        logger.info("Writing " + this);
        SSTableWriter writer = new SSTableWriter(cfs.getFlushPath(), columnFamilies.size(), StorageService.getPartitioner());

        DataOutputBuffer headerBuffer = new DataOutputBuffer(); // BIGDATA
        DataOutputBuffer buffer = new DataOutputBuffer();
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : columnFamilies.entrySet())
        {
            headerBuffer.reset(); // BIGDATA
            buffer.reset();
            /* serialize the cf with column indexes */
            ColumnFamily.serializer().serializeWithIndexes(entry.getValue(), headerBuffer, buffer, cfs.getCFMetaData().compressAlgo); // BIGDATA
            /* Now write the key and value to disk */
            writer.append(entry.getKey(), headerBuffer, buffer); // BIGDATA
        }

        SSTableReader ssTable = writer.closeAndOpenReader();
        logger.info(String.format("Completed flushing %s (%d bytes)",
                                  ssTable.getFilename(), new File(ssTable.getFilename()).length()));
        return ssTable;
    }

    public void flushAndSignal(final Condition condition, ExecutorService sorter, final ExecutorService writer)
    {
        cfs.getMemtablesPendingFlush().add(this); // it's ok for the MT to briefly be both active and pendingFlush
        writer.submit(new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                cfs.addSSTable(writeSortedContents());
                cfs.getMemtablesPendingFlush().remove(Memtable.this);
                condition.signalAll();
            }
        });
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s bytes, %s operations)",
                             cfs.getColumnFamilyName(), hashCode(), currentThroughput, currentOperations);
    }

    public Iterator<DecoratedKey> getKeyIterator(DecoratedKey startWith)
    {
        return columnFamilies.navigableKeySet().tailSet(startWith).iterator();
    }

    public boolean isClean()
    {
        return columnFamilies.isEmpty();
    }

    private String getTableName()
    {
        return cfs.getTable().name;
    }

    /**
     * obtain an iterator of columns in this memtable in the specified order starting from a given column.
     */
    public ColumnIterator getSliceIterator(ColumnFamily cf, SliceQueryFilter filter, AbstractType typeComparator)
    {
        final ColumnFamily columnFamily = cf == null ? ColumnFamily.create(getTableName(), filter.getColumnFamilyName()) : cf.cloneMeShallow();

        final IColumn columns[] = (cf == null ? columnFamily : cf).getSortedColumns().toArray(new IColumn[columnFamily.getSortedColumns().size()]);
        // TODO if we are dealing with supercolumns, we need to clone them while we have the read lock since they can be modified later
        if (filter.reversed)
            ArrayUtils.reverse(columns);
        IColumn startIColumn;
        final boolean isStandard = DatabaseDescriptor.getColumnFamilyType(getTableName(), filter.getColumnFamilyName()).equals("Standard");
        if (isStandard)
            startIColumn = new Column(filter.start);
        else
            startIColumn = new SuperColumn(filter.start, null); // ok to not have subcolumnComparator since we won't be adding columns to this object

        // can't use a ColumnComparatorFactory comparator since those compare on both name and time (and thus will fail to match
        // our dummy column, since the time there is arbitrary).
        Comparator<IColumn> comparator = filter.getColumnComparator(typeComparator);
        int index;
        if (filter.start.length == 0 && filter.reversed)
        {
            /* scan from the largest column in descending order */
            index = 0;
        }
        else
        {
            index = Arrays.binarySearch(columns, startIColumn, comparator);
        }
        final int startIndex = index < 0 ? -(index + 1) : index;

        return new AbstractColumnIterator()
        {
            private int curIndex_ = startIndex;

            public ColumnFamily getColumnFamily()
            {
                return columnFamily;
            }

            public boolean hasNext()
            {
                return curIndex_ < columns.length;
            }

            public IColumn next()
            {
                // clone supercolumns so caller can freely removeDeleted or otherwise mutate it
                return isStandard ? columns[curIndex_++] : ((SuperColumn)columns[curIndex_++]).cloneMe();
            }
        };
    }

    public ColumnIterator getNamesIterator(final ColumnFamily cf, final NamesQueryFilter filter)
    {
        final ColumnFamily columnFamily = cf == null ? ColumnFamily.create(getTableName(), filter.getColumnFamilyName()) : cf.cloneMeShallow();
        final boolean isStandard = DatabaseDescriptor.getColumnFamilyType(getTableName(), filter.getColumnFamilyName()).equals("Standard");

        return new SimpleAbstractColumnIterator()
        {
            private Iterator<byte[]> iter = filter.columns.iterator();
            private byte[] current;

            public ColumnFamily getColumnFamily()
            {
                return columnFamily;
            }

            protected IColumn computeNext()
            {
                if (cf == null)
                {
                    return endOfData();
                }
                while (iter.hasNext())
                {
                    current = iter.next();
                    IColumn column = cf.getColumn(current);
                    if (column != null)
                        // clone supercolumns so caller can freely removeDeleted or otherwise mutate it
                        return isStandard ? column : ((SuperColumn)column).cloneMe();
                }
                return endOfData();
            }
        };
    }

    public ColumnFamily getColumnFamily(String key)
    {
        return columnFamilies.get(partitioner.decorateKey(key));
    }

    void clearUnsafe()
    {
        columnFamilies.clear();
    }

    public boolean isExpired()
    {
        return System.currentTimeMillis() > creationTime + DatabaseDescriptor.getMemtableLifetimeMS();
    }
}
