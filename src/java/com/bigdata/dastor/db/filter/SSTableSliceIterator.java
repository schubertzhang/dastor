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


import java.util.*;
import java.io.DataInputStream;
import java.io.IOException;

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
import com.google.common.collect.AbstractIterator;

/**
 *  A Column Iterator over SSTable
 */
class SSTableSliceIterator extends AbstractIterator<IColumn> implements ColumnIterator
{
    // BIGDATA:
    private static final Logger logger = Logger.getLogger(SSTableSliceIterator.class);
    
    private final boolean reversed;
    private final byte[] startColumn;
    private final byte[] finishColumn;
    private final AbstractType comparator;
    private ColumnGroupReaderInterface reader; // BIGDATA: chance to use a interface, since we will support different format
    
    // BIGDATA:
    private int dataSize;
    private long dataStart;
    private byte rowFormat;
    private long realDataStart;
    
    public SSTableSliceIterator(SSTableReader ssTable, String key, byte[] startColumn, byte[] finishColumn, boolean reversed)
    throws IOException
    {
        this.reversed = reversed;

        /* Morph key into actual key based on the partition type. */
        DecoratedKey decoratedKey = ssTable.getPartitioner().decorateKey(key);
        FileDataInput fdi = ssTable.getFileDataInput(decoratedKey, DatabaseDescriptor.getSlicedReadBufferSizeInKB() * 1024);
        this.comparator = ssTable.getColumnComparator();
        this.startColumn = startColumn;
        this.finishColumn = finishColumn;
        if (fdi != null)
        {
            // BIGDATA: move up here
            DecoratedKey keyInDisk = ssTable.getPartitioner().convertFromDiskFormat(fdi.readUTF());
            assert keyInDisk.equals(decoratedKey)
                   : String.format("%s != %s in %s", keyInDisk, decoratedKey, fdi.getPath());

            dataSize = fdi.readInt(); // row data size
            dataStart = fdi.getAbsolutePosition();
            rowFormat = ColumnFamily.serializer().deserializeRowFormat(fdi);
            realDataStart = fdi.getAbsolutePosition();
            
            // !BIGDATA: branch out to the different code process for new rowformat/compression.
            if (ColumnFamily.serializer().isNewRowFormat(rowFormat))
            {
                // new row format
                reader = new BigdataColumnGroupReader(ssTable, decoratedKey, fdi);
            }
            else
            {
                // old row format
                reader = new ColumnGroupReader(ssTable, decoratedKey, fdi);
            }
        }
    }

    private boolean isColumnNeeded(IColumn column)
    {
        if (startColumn.length == 0 && finishColumn.length == 0)
            return true;
        else if (startColumn.length == 0 && !reversed)
            return comparator.compare(column.name(), finishColumn) <= 0;
        else if (startColumn.length == 0 && reversed)
            return comparator.compare(column.name(), finishColumn) >= 0;
        else if (finishColumn.length == 0 && !reversed)
            return comparator.compare(column.name(), startColumn) >= 0;
        else if (finishColumn.length == 0 && reversed)
            return comparator.compare(column.name(), startColumn) <= 0;
        else if (!reversed)
            return comparator.compare(column.name(), startColumn) >= 0 && comparator.compare(column.name(), finishColumn) <= 0;
        else // if reversed
            return comparator.compare(column.name(), startColumn) <= 0 && comparator.compare(column.name(), finishColumn) >= 0;
    }

    public ColumnFamily getColumnFamily()
    {
        return reader == null ? null : reader.getEmptyColumnFamily();
    }

    protected IColumn computeNext()
    {
        if (reader == null)
            return endOfData();

        while (true)
        {
            IColumn column = reader.pollColumn();
            if (column == null)
                return endOfData();
            if (isColumnNeeded(column))
                return column;
        }
    }

    public void close() throws IOException
    {
        if (reader != null)
            reader.close();
    }

    /**
     *  This is a reader that finds the block for a starting column and returns
     *  blocks before/after it for each next call. This function assumes that
     *  the CF is sorted by name and exploits the name index.
     */
    class ColumnGroupReader implements ColumnGroupReaderInterface
    {
        private final ColumnFamily emptyColumnFamily;

        private final List<IndexHelper.IndexInfo> indexes;
        private final FileDataInput file;

        private int curRangeIndex;
        private Deque<IColumn> blockColumns = new ArrayDeque<IColumn>();
        private SSTableReader ssTable;
        
        public ColumnGroupReader(SSTableReader ssTable, DecoratedKey key, FileDataInput input) throws IOException
        {
            this.file = input;
            this.ssTable = ssTable;
            assert file.getAbsolutePosition() == realDataStart;
            
            // BIGDATA: some code move up.            
            
            IndexHelper.skipBloomFilter(file);
            indexes = IndexHelper.deserializeIndex(file);

            emptyColumnFamily = ColumnFamily.serializer().deserializeFromSSTableNoColumns(ssTable.makeColumnFamily(), file);
            file.readInt(); // column count

            file.mark();
            curRangeIndex = IndexHelper.indexFor(startColumn, indexes, comparator, reversed);
            if (reversed && curRangeIndex == indexes.size())
                curRangeIndex--;
        }

        public ColumnFamily getEmptyColumnFamily()
        {
            return emptyColumnFamily;
        }

        public IColumn pollColumn()
        {
            IColumn column = blockColumns.poll();
            if (column == null)
            {
                try
                {
                    if (getNextBlock())
                        column = blockColumns.poll();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return column;
        }

        public boolean getNextBlock() throws IOException
        {
            if (curRangeIndex < 0 || curRangeIndex >= indexes.size())
                return false;

            /* seek to the correct offset to the data, and calculate the data size */
            IndexHelper.IndexInfo curColPosition = indexes.get(curRangeIndex);

            /* see if this read is really necessary. */
            if (reversed)
            {
                if ((finishColumn.length > 0 && comparator.compare(finishColumn, curColPosition.lastName) > 0) ||
                    (startColumn.length > 0 && comparator.compare(startColumn, curColPosition.firstName) < 0))
                    return false;
            }
            else
            {
                if ((startColumn.length > 0 && comparator.compare(startColumn, curColPosition.lastName) > 0) ||
                    (finishColumn.length > 0 && comparator.compare(finishColumn, curColPosition.firstName) < 0))
                    return false;
            }

            boolean outOfBounds = false;

            file.reset();
            long curOffset = file.skipBytes((int) curColPosition.offset); 
            assert curOffset == curColPosition.offset;
            while (file.bytesPastMark() < curColPosition.offset + curColPosition.width && !outOfBounds)
            {
                IColumn column = emptyColumnFamily.getColumnSerializer().deserialize(file);
                if (reversed)
                    blockColumns.addFirst(column);
                else
                    blockColumns.addLast(column);

                /* see if we can stop seeking. */
                if (!reversed && finishColumn.length > 0)
                    outOfBounds = comparator.compare(column.name(), finishColumn) >= 0;
                else if (reversed && startColumn.length > 0)
                    outOfBounds = comparator.compare(column.name(), startColumn) >= 0;
                    
                if (outOfBounds)
                    break;
            }

            if (reversed)
                curRangeIndex--;
            else
                curRangeIndex++;
            return true;
        }

        public void close() throws IOException
        {
            file.close();
        }
    }
    
    
    /*
     * !BIGDATA:
     */
    interface ColumnGroupReaderInterface
    {
        public ColumnFamily getEmptyColumnFamily();
        public IColumn pollColumn();
        public boolean getNextBlock() throws IOException;
        public void close() throws IOException;
    }
    
    /*
     * !BIGDATA:
     */
    class BigdataColumnGroupReader implements ColumnGroupReaderInterface
    {
        private final ColumnFamily emptyColumnFamily;

        private final List<IndexHelper.IndexInfo> indexes;
        private final FileDataInput file;
        private final SSTableReader ssTable;
        private final long firstBlockPos;
        private final ColumnFamilySerializer.CompressionContext compressContext;
        
        private int curRangeIndex;
        private Deque<IColumn> blockColumns = new ArrayDeque<IColumn>();
        
        public BigdataColumnGroupReader(SSTableReader ssTable, DecoratedKey key, FileDataInput input) throws IOException
        {
            this.file = input;
            this.ssTable = ssTable;
            assert file.getAbsolutePosition() == realDataStart;
            
            if (ColumnFamily.serializer().isNewRowFormatIndexAtEnd(rowFormat))
            {
                ////// HEADER //////
                
                // skip bloom filter
                IndexHelper.skipBloomFilter(file);
                
                // read deletion meta info
                emptyColumnFamily = ColumnFamily.serializer().deserializeFromSSTableNoColumns(ssTable.makeColumnFamily(), file);
                file.readInt(); // column count

                // the position of the first block
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
                indexes = IndexHelper.deserializeIndex(file);
            }
            else
            {
                // skip bloom filter
                IndexHelper.skipBloomFilter(file);
                
                //read in index
                indexes = IndexHelper.deserializeIndex(file);
                
                // read deletion meta info            
                emptyColumnFamily = ColumnFamily.serializer().deserializeFromSSTableNoColumns(ssTable.makeColumnFamily(), file);
                file.readInt(); // column count
    
                // the position of the first block
                firstBlockPos = file.getAbsolutePosition();
            }
            
            curRangeIndex = IndexHelper.indexFor(startColumn, indexes, comparator, reversed);
            if (reversed && curRangeIndex == indexes.size())
                curRangeIndex--;

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

        public ColumnFamily getEmptyColumnFamily()
        {
            return emptyColumnFamily;
        }

        public IColumn pollColumn()
        {
            IColumn column = blockColumns.poll();
            if (column == null)
            {
                try
                {
                    if (getNextBlock())
                        column = blockColumns.poll();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return column;
        }

        public boolean getNextBlock() throws IOException
        {
            if (curRangeIndex < 0 || curRangeIndex >= indexes.size())
                return false;

            /* seek to the correct offset to the data, and calculate the data size */
            IndexHelper.IndexInfo curColPosition = indexes.get(curRangeIndex);

            /* see if this read is really necessary. */
            if (reversed)
            {
                if ((finishColumn.length > 0 && comparator.compare(finishColumn, curColPosition.lastName) > 0) ||
                    (startColumn.length > 0 && comparator.compare(startColumn, curColPosition.firstName) < 0))
                    return false;
            }
            else
            {
                if ((startColumn.length > 0 && comparator.compare(startColumn, curColPosition.lastName) > 0) ||
                    (finishColumn.length > 0 && comparator.compare(finishColumn, curColPosition.firstName) < 0))
                    return false;
            }

            boolean outOfBounds = false;
            
            // seek to current block 
            // curIndexInfo.offset is the relative offset from the first block
            file.seek(firstBlockPos + curColPosition.offset);

            // read all columns of current block into memory!
            DataInputStream blockIn = ColumnFamily.serializer().getBlockInputStream(file, curColPosition.sizeOnDisk, compressContext);
            try
            {
                int size = 0;
                while ((size < curColPosition.width) && !outOfBounds)
                {
                    IColumn column = emptyColumnFamily.getColumnSerializer().deserialize(blockIn);
                    size += column.serializedSize();
                    if (reversed)
                        blockColumns.addFirst(column);
                    else
                        blockColumns.addLast(column);
                    
                    /* see if we can stop seeking. */
                    if (!reversed && finishColumn.length > 0)
                        outOfBounds = comparator.compare(column.name(), finishColumn) >= 0;
                    else if (reversed && startColumn.length > 0)
                        outOfBounds = comparator.compare(column.name(), startColumn) >= 0;
                        
                    if (outOfBounds)
                        break;
                }
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

            if (reversed)
                curRangeIndex--;
            else
                curRangeIndex++;
            return true;
        }

        public void close() throws IOException
        {
            file.close();
        }
    }
}
