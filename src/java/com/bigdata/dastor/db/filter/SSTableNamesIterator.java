package com.bigdata.dastor.db.filter;
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


import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.log4j.Logger;

import com.bigdata.dastor.config.DatabaseDescriptor;
import com.bigdata.dastor.db.ColumnFamily;
import com.bigdata.dastor.db.ColumnFamilySerializer;
import com.bigdata.dastor.db.DecoratedKey;
import com.bigdata.dastor.db.IColumn;
import com.bigdata.dastor.db.marshal.AbstractType;
import com.bigdata.dastor.io.*;
import com.bigdata.dastor.io.compress.Compression;
import com.bigdata.dastor.io.util.FileDataInput;
import com.bigdata.dastor.utils.BloomFilter;

public class SSTableNamesIterator extends SimpleAbstractColumnIterator
{
    // BIGDATA:
    private static final Logger logger = Logger.getLogger(SSTableNamesIterator.class);
    
    private ColumnFamily cf;
    private Iterator<IColumn> iter;
    public final SortedSet<byte[]> columns;
    
    // BIGDATA:
    private int dataSize;
    private long dataStart;
    private byte rowFormat;
    private long realDataStart;

    public SSTableNamesIterator(SSTableReader ssTable, String key, SortedSet<byte[]> columnNames) throws IOException
    {
        assert columnNames != null;
        this.columns = columnNames;

        DecoratedKey decoratedKey = ssTable.getPartitioner().decorateKey(key);

        FileDataInput file = ssTable.getFileDataInput(decoratedKey, DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
        if (file == null)
            return;
        try
        {
            DecoratedKey keyInDisk = ssTable.getPartitioner().convertFromDiskFormat(file.readUTF());
            assert keyInDisk.equals(decoratedKey) 
                   : String.format("%s != %s in %s", keyInDisk, decoratedKey, file.getPath());
            dataSize = file.readInt(); // BIGDATA: data size
            dataStart = file.getAbsolutePosition(); // BIGDATA

            // BIGDATA: the first int of row data is row format
            rowFormat = ColumnFamily.serializer().deserializeRowFormat(file);
            realDataStart = file.getAbsolutePosition();
            
            // !BIGDATA: branch out to the different code process for new rowformat/compression.
            if (ColumnFamily.serializer().isNewRowFormat(rowFormat))
            {
                bigdataSSTableNamesIterator(file, ssTable, decoratedKey, columnNames);
                return; // PAY ATTENTION: even return, the "finally" part will still be executed.
            }
            
            /* Read the bloom filter and index summarizing the columns */
            BloomFilter bf = IndexHelper.defreezeBloomFilter(file);
            List<IndexHelper.IndexInfo> indexList = IndexHelper.deserializeIndex(file);

            cf = ColumnFamily.serializer().deserializeFromSSTableNoColumns(ssTable.makeColumnFamily(), file);

            // we can stop early if bloom filter says none of the columns actually exist -- but,
            // we can't stop before initializing the cf above, in case there's a relevant tombstone
            List<byte[]> filteredColumnNames = new ArrayList<byte[]>(columnNames.size());
            for (byte[] name : columnNames)
            {
                if (bf.isPresent(name))
                {
                    filteredColumnNames.add(name);
                }
            }
            if (filteredColumnNames.isEmpty())
                return;

            file.readInt(); // column count

            /* get the various column ranges we have to read */
            AbstractType comparator = ssTable.getColumnComparator();
            SortedSet<IndexHelper.IndexInfo> ranges = new TreeSet<IndexHelper.IndexInfo>(IndexHelper.getComparator(comparator));
            for (byte[] name : filteredColumnNames)
            {
                int index = IndexHelper.indexFor(name, indexList, comparator, false);
                if (index == indexList.size())
                    continue;
                IndexHelper.IndexInfo indexInfo = indexList.get(index);
                if (comparator.compare(name, indexInfo.firstName) < 0)
                   continue;
                ranges.add(indexInfo);
            }

            file.mark();
            for (IndexHelper.IndexInfo indexInfo : ranges)
            {
                file.reset();
                long curOffsert = file.skipBytes((int)indexInfo.offset);
                assert curOffsert == indexInfo.offset;
                // TODO only completely deserialize columns we are interested in
                while (file.bytesPastMark() < indexInfo.offset + indexInfo.width)
                {
                    final IColumn column = cf.getColumnSerializer().deserialize(file);
                    // we check vs the original Set, not the filtered List, for efficiency
                    if (columnNames.contains(column.name()))
                    {
                        cf.addColumn(column);
                    }
                }
            }
        }
        finally
        {
            file.close();
        }

        iter = cf.getSortedColumns().iterator();
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    protected IColumn computeNext()
    {
        if (iter == null || !iter.hasNext())
            return endOfData();
        return iter.next();
    }
    
    /*
     * !BIGDATA:
     * for new row format
     */
    private void bigdataSSTableNamesIterator(FileDataInput file, SSTableReader ssTable, DecoratedKey decoratedKey, SortedSet<byte[]> columnNames) throws IOException
    {
        assert file.getAbsolutePosition() == realDataStart;

        List<byte[]> filteredColumnNames;
        long firstBlockPos;
        List<IndexHelper.IndexInfo> indexList;
        
        if (ColumnFamily.serializer().isNewRowFormatIndexAtEnd(rowFormat))
        {
            ////// HEADER //////
            
            /* Read the bloom filter summarizing the columns */
            BloomFilter bf = IndexHelper.defreezeBloomFilter(file);

            // read the deletion info
            cf = ColumnFamily.serializer().deserializeFromSSTableNoColumns(ssTable.makeColumnFamily(), file);

            filteredColumnNames = new ArrayList<byte[]>(columnNames.size());
            for (byte[] name : columnNames)
            {
                if (bf.isPresent(name))
                {
                    filteredColumnNames.add(name);
                }
            }
            if (filteredColumnNames.isEmpty())
                return;

            // read column count (not used here)
            file.readInt();

            // get the position of the first block
            firstBlockPos = file.getAbsolutePosition();
            
            ////// TRAILER //////
            
            // seek to the trailer
            // THE FIRST SEEK!!!
            file.seek(dataStart + dataSize - (Integer.SIZE/Byte.SIZE));
            
            // index size (with column index size's int)
            int indexSize = file.readInt();
            
            ////// INDEX //////
            
            // seek to index position
            // THE SECOND SEEK!!!
            file.seek(dataStart + dataSize - (Integer.SIZE/Byte.SIZE) - indexSize);

            // read index into memory
            indexList = IndexHelper.deserializeIndex(file);
        }
        else
        {
            /* Read the bloom filter summarizing the columns */
            BloomFilter bf = IndexHelper.defreezeBloomFilter(file);

            // read index into memory
            indexList = IndexHelper.deserializeIndex(file);
            
            // read the deletion info
            cf = ColumnFamily.serializer().deserializeFromSSTableNoColumns(ssTable.makeColumnFamily(), file);

            filteredColumnNames = new ArrayList<byte[]>(columnNames.size());
            for (byte[] name : columnNames)
            {
                if (bf.isPresent(name))
                {
                    filteredColumnNames.add(name);
                }
            }
            if (filteredColumnNames.isEmpty())
                return;

            // read column count (not used here)
            file.readInt();

            // get the position of the first block
            firstBlockPos = file.getAbsolutePosition();
        }

        /* get the various column ranges/blocks we have to read */
        AbstractType comparator = ssTable.getColumnComparator();
        SortedSet<IndexHelper.IndexInfo> ranges = new TreeSet<IndexHelper.IndexInfo>(IndexHelper.getComparator(comparator));
        for (byte[] name : filteredColumnNames)
        {
            int index = IndexHelper.indexFor(name, indexList, comparator, false);
            if (index == indexList.size())
                continue;
            IndexHelper.IndexInfo indexInfo = indexList.get(index);
            if (comparator.compare(name, indexInfo.firstName) < 0)
               continue;
            ranges.add(indexInfo);
        }

        if (ranges.size() > 0)
        {
            // compression algorithm used when writing
            Compression.Algorithm compressAlgo;
            try
            {
                compressAlgo = Compression.getCompressionAlgorithmById(ColumnFamily.serializer().getNewRowFormatCompressAlgo(rowFormat));
            }
            catch (IllegalArgumentException e)
            {
                logger.error(e.toString());
                throw new IOException(e);
            }
            ColumnFamilySerializer.CompressionContext compressContext = ColumnFamilySerializer.CompressionContext.getInstance(compressAlgo);
            
            for (IndexHelper.IndexInfo indexInfo : ranges)
            {
                // seek to current block 
                // curIndexInfo.offset is the relative offset from the first block
                file.seek(firstBlockPos + indexInfo.offset);
                
                // read all columns of current block into memory!
                DataInputStream blockIn = ColumnFamily.serializer().getBlockInputStream(file, indexInfo.sizeOnDisk, compressContext);
                try
                {
                    int size = 0;
                    while (size < indexInfo.width)
                    {
                        IColumn column = cf.getColumnSerializer().deserialize(blockIn);
                        size += column.serializedSize();
                        // we check vs the original Set, not the filtered List, for efficiency
                        if (columnNames.contains(column.name()))
                        {
                            cf.addColumn(column);
                        }
                    }
                    assert size == indexInfo.width;
                }
                catch (IOException e)
                {
                    logger.error(e.toString());
                    throw e;
                }
                finally
                {
                    ColumnFamily.serializer().releaseBlockInputStream(blockIn, compressContext);
                }
            }
        }

        iter = cf.getSortedColumns().iterator();
    }
}
