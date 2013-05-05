package com.bigdata.dastor.io;
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
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.dastor.db.ColumnFamily;
import com.bigdata.dastor.db.ColumnFamilySerializer;
import com.bigdata.dastor.db.DecoratedKey;
import com.bigdata.dastor.db.IColumn;
import com.bigdata.dastor.io.compress.Compression;
import com.bigdata.dastor.io.util.BufferedRandomAccessFile;
import com.bigdata.dastor.service.StorageService;
import com.google.common.collect.AbstractIterator;

public class IteratingRow extends AbstractIterator<IColumn> implements Comparable<IteratingRow>
{
    // BIGDATA:
    private static final Logger logger = Logger.getLogger(IteratingRow.class);
    
    private final DecoratedKey key;
    private final long finishedAt;
    private final BufferedRandomAccessFile file;
    public final SSTableReader sstable;
    
    // BIGDATA:
    private final long dataStart;
    private final int dataSize;
    private final byte rowFormat;
    private final long realDataStart;
    
    // BIGDATA:
    private BigdataColumnBlockReader bigdataBlockReader = null;
        
    public IteratingRow(BufferedRandomAccessFile file, SSTableReader sstable) throws IOException
    {
        this.file = file;
        this.sstable = sstable;

        key = StorageService.getPartitioner().convertFromDiskFormat(file.readUTF());
        dataSize = file.readInt();
        dataStart = file.getFilePointer();
        finishedAt = dataStart + dataSize;
        
        // BIGDATA:
        // the first byte of row data is row format
        rowFormat = ColumnFamily.serializer().deserializeRowFormat(file);
        realDataStart = file.getFilePointer();
        
        if (ColumnFamily.serializer().isNewRowFormat(rowFormat))
        {
            bigdataBlockReader = new BigdataColumnBlockReader();
        }
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public String getPath()
    {
        return file.getPath();
    }

    public void echoData(DataOutput out) throws IOException
    {
        file.seek(dataStart);
        while (file.getFilePointer() < finishedAt)
        {
            out.write(file.readByte());
        }
    }

    // TODO r/m this and make compaction merge columns iteratively for CASSSANDRA-16
    public ColumnFamily getColumnFamily() throws IOException
    {
        if (bigdataBlockReader != null) // new row format
        {
            bigdataBlockReader.reset(); // PAY ATTENTION! allow re-entry
            ColumnFamily cf = bigdataBlockReader.cfNoColumns;
            IColumn column = bigdataBlockReader.pollColumn();
            while (column != null)
            {
                cf.addColumn(column);
                column = bigdataBlockReader.pollColumn();
            }
            return cf;
        }

        // old row format
        file.seek(realDataStart);
        IndexHelper.skipBloomFilter(file);
        IndexHelper.skipIndex(file);
        return ColumnFamily.serializer().deserializeFromSSTable(sstable, file);
    }

    public void skipRemaining() throws IOException
    {
        file.seek(finishedAt);
    }

    public long getEndPosition()
    {
        return finishedAt;
    }

    protected IColumn computeNext()
    {
        if (bigdataBlockReader != null) // new row format
        {
            try
            {
                IColumn column = bigdataBlockReader.pollColumn();
                if (column == null)
                {
                    return endOfData();
                }
                return column;
            }
            catch (IOException e)
            {
                logger.error(e.toString());
                throw new RuntimeException(e);
            }
        }
        
        // old row format
        try
        {
            assert file.getFilePointer() <= finishedAt;
            if (file.getFilePointer() == finishedAt)
            {
                return endOfData();
            }

            return sstable.getColumnSerializer().deserialize(file);
        }
        catch (IOException e)
        {
            logger.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    public int compareTo(IteratingRow o)
    {
        return key.compareTo(o.key);
    }
    

    // !BIGDATA:
    // PAY ATTENTION: This is a inner class.
    private class BigdataColumnBlockReader
    {
        private final ColumnFamily cfNoColumns;
        private final int columnCount;
        private final List<IndexHelper.IndexInfo> indexList;
        private final long firstBlockPos;
        private final ColumnFamilySerializer.CompressionContext compressContext;
                
        private int curIndexId = 0;
        private Deque<IColumn> blockColumns = new ArrayDeque<IColumn>();

        private BigdataColumnBlockReader() throws IOException
        {
            // the file's position must be right at row's data.
            assert file.getFilePointer() == realDataStart;
            
            if (ColumnFamily.serializer().isNewRowFormatIndexAtEnd(rowFormat))
            {
                ////// HEADER //////
                
                // skip bloom filter
                IndexHelper.skipBloomFilter(file);

                // read the deletion meta
                cfNoColumns = ColumnFamily.serializer().deserializeFromSSTableNoColumns(sstable.makeColumnFamily(), file);

                // column count
                columnCount = file.readInt();
                
                // position of the first block
                firstBlockPos = file.getFilePointer();
                
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
                
                // now, the filePointer (position) is at the trailer.
                // but don't worry, we will seek to the block position when read block.
            }
            else
            {
                // skip bloom filter
                IndexHelper.skipBloomFilter(file);
                // read in index
                indexList = IndexHelper.deserializeIndex(file);
                // read the deletion meta
                cfNoColumns = ColumnFamily.serializer().deserializeFromSSTableNoColumns(sstable.makeColumnFamily(), file);
                // column count
                columnCount = file.readInt();
                // position of the first block
                firstBlockPos = file.getFilePointer();
            }
            
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
            compressContext = ColumnFamilySerializer.CompressionContext.getInstance(compressAlgo);
        }
        
        private void reset()
        {
            // to the first block
            curIndexId = 0;

            // clear columns if there are
            blockColumns.clear();
            cfNoColumns.clear();
        }
                
        private IColumn pollColumn() throws IOException
        {
            IColumn column = blockColumns.poll();
            while (column == null)
            {
                if (!getNextBlock())
                {
                    // finished the last block
                    return null;
                }
                column = blockColumns.poll();
            }
            return column;
        }

        private boolean getNextBlock() throws IOException
        {
            if ((curIndexId < 0) || (curIndexId >= indexList.size()))
            {
                // finished the last block
                return false;
            }

            // get current block index info
            IndexHelper.IndexInfo curIndexInfo = indexList.get(curIndexId);

            // seek to current block 
            // curIndexInfo.offset is the relative offset from the first block
            // THE THRID SEEK!!!
            file.seek(firstBlockPos + curIndexInfo.offset);
            
            // read all columns of current block into memory!
            DataInputStream blockIn = ColumnFamily.serializer().getBlockInputStream(file, curIndexInfo.sizeOnDisk, compressContext);
            try
            {
                int size = 0;
                while (size < curIndexInfo.width)
                {
                    IColumn column = cfNoColumns.getColumnSerializer().deserialize(blockIn);
                    size += column.serializedSize();
                    blockColumns.addLast(column);
                }
                assert size == curIndexInfo.width;
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
            
            // go to next block in next time
            curIndexId++;
            return true;
        }
    }
}
