package com.bigdata.dastor.db;
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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInput;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.log4j.Logger;

import com.bigdata.dastor.config.DatabaseDescriptor;
import com.bigdata.dastor.db.marshal.AbstractType;
import com.bigdata.dastor.io.ICompactSerializer2;
import com.bigdata.dastor.io.IndexHelper;
import com.bigdata.dastor.io.SSTableReader;
import com.bigdata.dastor.io.compress.Compression;
import com.bigdata.dastor.io.util.BoundedFileDataInputStream;
import com.bigdata.dastor.io.util.DataOutputBuffer;
import com.bigdata.dastor.io.util.FileDataInput;

public class ColumnFamilySerializer implements ICompactSerializer2<ColumnFamily>
{
    private static final Logger logger = Logger.getLogger(ColumnFamilySerializer.class);
    
    /**
     * ************************************************************************
     * PAY ATTENTION: This method is only internally used for message.
     * [1] Row-Mutation (RowMutationSerializer)
     * [2] Query-Result (RowSerializer)
     * ************************************************************************
     * 
     * Serialized ColumnFamily format:
     *
     * [serialized for intra-node writes only, e.g. returning a query result]
     * <cf name>
     * <cf type [super or standard]>
     * <cf comparator name>
     * <cf subcolumn comparator name>
     *
     * [in sstable only]
     * <column bloom filter>
     * <sparse column index, start/finish columns every ColumnIndexSizeInKB of data>
     *
     * [always present]
     * <local deletion time>
     * <client-provided deletion time>
     * <column count>
     * <columns, serialized individually>
    */
    public void serialize(ColumnFamily columnFamily, DataOutput dos)
    {
        try
        {
            if (columnFamily == null)
            {
                dos.writeUTF(""); // not a legal CF name
                return;
            }

            dos.writeUTF(columnFamily.name());
            dos.writeUTF(columnFamily.type_);
            dos.writeUTF(columnFamily.getComparatorName());
            dos.writeUTF(columnFamily.getSubComparatorName());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        serializeForSSTable(columnFamily, dos);
    }

    /*
     * ************************************************************************
     * PAY ATTENTION: This method is now internally used for message.
     * This is also the old format in SSTable.
     * [1] Row-Mutation (RowMutationSerializer)
     * [2] Query-Result (RowSerializer)
     * ************************************************************************
     */
    private void serializeForSSTable(ColumnFamily columnFamily, DataOutput dos) // BIGDATA: chance to private
    {
        try
        {
            dos.writeInt(columnFamily.localDeletionTime.get());
            dos.writeLong(columnFamily.markedForDeleteAt.get());

            Collection<IColumn> columns = columnFamily.getSortedColumns();
            dos.writeInt(columns.size());
            for (IColumn column : columns)
            {
                columnFamily.getColumnSerializer().serialize(column, dos);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * ************************************************************************
     * !BIGDATA:
     * PAY ATTENTION: For both new and old row format.
     *   bit0~3: compressAlgo
     *   bit6  : index at end
     *   bit7  : new format
     * ************************************************************************
     */
    public void serializeRowFormat(boolean newRowFormat, boolean newIndexAtEnd, 
                                   int compressAlgo, DataOutput dos)
    {
        int rowFormat = 0;
        if (newRowFormat)
        {
            rowFormat |= (1<<7);
            if (newIndexAtEnd)
                rowFormat |= (1<<6);
            rowFormat |= (compressAlgo & 0x0F);
        }
        
        try
        {
            dos.writeByte(rowFormat);
        }
        catch (IOException e)
        {
            logger.error(e.toString());
            throw new RuntimeException(e);
        }
    }
    
    /**
     * ************************************************************************
     * !BIGDATA:
     * PAY ATTENTION: For both new and old row format.
     * ************************************************************************
     */
    public byte deserializeRowFormat(DataInput dis)
    {
        try
        {
           return dis.readByte();
        }
        catch (IOException e)
        {
            logger.error(e.toString());
            throw new RuntimeException(e);
        }
    }
    
    public boolean isNewRowFormat(byte rowFormat)
    {
        return ( (rowFormat & (1<<7)) != 0 );
    }
    
    public boolean isNewRowFormatIndexAtEnd(byte rowFormat)
    {
        return ( (rowFormat & (1<<6)) != 0 );
    }
    
    public int getNewRowFormatCompressAlgo(byte rowFormat)
    {
        return (int)(rowFormat & 0x0F);
    }
    
    /**
     * ************************************************************************
     * PAY ATTENTION: The old method to write column family row 
     * (column index and columns) to SSTable.
     * ************************************************************************
     *
     * the old format of ColumnFamily row in SSTable:
     * |------------------------|<--------------------------|
     * |     bloom filter       | (int for len, and content)|
     * |------------------------|                           |
     * |      index size        | (byte size of index)      |<- old ColumnIndexer.serialize(...)
     * |------------------------|                           |
     * |    index of block 0    |                           |
     * |----------    ----------|                           |
     * |    index of block 1    |                           |
     * |------------------------|<--------------------------- <<-- serializeForSSTable(...)
     * |     deletion meta      | localDeletionTime(int) and markedForDeleteAt(long)
     * |------------------------| 
     * |     column count       | (int)
     * |------------------------|<----- COLUMN BLOCKS START POSITION(CBSP)
     * |     column block 0     |       BLOCK-INDEX OFFSET FROM CBSP
     * |     (uncompressed)     |
     * |----------    ----------|
     * |     column block 1     |
     * |     (uncompressed)     |
     * |------------------------|
     *
     * BIGDATA: change method name to oldSerializeWithIndexes from serializeWithIndexes
     */
    public void oldSerializeWithIndexes(ColumnFamily columnFamily, DataOutput dos)
    {        
        ColumnIndexer.serialize(columnFamily, dos);
        serializeForSSTable(columnFamily, dos);
    }
    
    /**
     * !BIGDATA:
     * ************************************************************************
     * New serialized ColumnFamily row with column index.
     * The new method to write column family row (column index and columns)
     * to SSTable.
     * ************************************************************************
     * 
     * the new format of ColumnFamily row:
     * |------------------------|<--------------------------|
     * |     bloom filter       | (int for len, and content)|
     * |------------------------|                           |
     * |      index size        | (byte size of index)      |
     * |------------------------|                           |
     * |    index of block 0    |                           |
     * |----------    ----------|                           |
     * |    index of block 1    |                           |
     * |------------------------|<---------------------------
     * |     deletion meta      | localDeletionTime(int) and markedForDeleteAt(long)
     * |------------------------| 
     * |     column count       | (int)
     * |------------------------|<----- COLUMN BLOCKS START POSITION(CBSP)
     * |     column block 0     |       BLOCK-INDEX OFFSET FROM CBSP
     * |     (compressed)       |
     * |----------    ----------|
     * |     column block 1     |
     * |     (compressed)       |
     * |------------------------|
     * 
     * @param columnFamily
     * @param dos
     * @throws IOException
     */
    public void bigdataSerializeWithIndexes(ColumnFamily columnFamily, DataOutputBuffer headerDos, DataOutputBuffer dos, Compression.Algorithm compressAlgo)
    {
        // get the sorted columns from column family row
        Collection<IColumn> columns = columnFamily.getSortedColumns();
        
        // create and serialize bloom filter
        BigdataColumnIndexer.createAndSerializeBloomFiliter(columns, headerDos);

        /*
         * Maintains a list of Column IndexInfo objects for the columns in this
         * column family row. The key is the column name and the position is the
         * relative offset of that column name from the start of the list.
         * We do this so that we don't read all the columns into memory.
         */
        List<IndexHelper.IndexInfo> indexList = new ArrayList<IndexHelper.IndexInfo>();
        
        // different column family use different compression algorithm.
        CompressionContext context = CompressionContext.getInstance(compressAlgo);
        
        try
        {
            // deletion meta information
            dos.writeInt(columnFamily.localDeletionTime.get());
            dos.writeLong(columnFamily.markedForDeleteAt.get());
            
            // column count
            dos.writeInt(columns.size());

            // the current column
            IColumn column = null;
            // the size of serialized column index, computed up front
            int indexSizeInBytes = 0;
            // the position of first block, at where the column blocks start.
            int firstBlockPos = dos.getLength();
            // the first column of current block
            IColumn blockFirstColumn = null;
            // the start position of current block
            // the column index will store the offset from firstBlockPos
            int blockStartPos = firstBlockPos;
            // uncompressed current block size
            int blockSize = 0;
            // compressed output stream of current block 
            DataOutputStream blockOut = null;
            
            for (Iterator<IColumn> it = columns.iterator(); it.hasNext();)
            {
                column = it.next();

                if ((blockFirstColumn == null) && (blockOut == null))
                {
                    // start a new block
                    blockFirstColumn = column;
                    blockStartPos = dos.getLength();
                    blockSize = 0;
                    // get a new block output stream
                    blockOut = getBlockOutputStream(dos, context);
                }
                
                // serialize column
                columnFamily.getColumnSerializer().serialize(column, blockOut);
                // uncompressed block size
                blockSize += column.serializedSize();
                
                // if we hit the block size that we have to index after, go ahead and index it.
                if (blockSize >= DatabaseDescriptor.getColumnIndexSize())
                {
                    int blockWidth = releaseBlockOutputStream(blockOut, context);
                    assert blockWidth == blockSize;
                    blockOut = null;

                    int blockEndPos = dos.getLength();
                    IndexHelper.IndexInfo cIndexInfo =
                            new IndexHelper.IndexInfo(blockFirstColumn.name(), column.name(), blockStartPos-firstBlockPos, blockWidth, blockEndPos-blockStartPos);
                    indexList.add(cIndexInfo);
                    indexSizeInBytes += cIndexInfo.serializedSize();
                    
                    // to next block
                    blockFirstColumn = null;
                }
            }
            
            if (blockOut != null)
            {
                int blockWidth = releaseBlockOutputStream(blockOut, context);
                assert blockWidth == blockSize;
                blockOut = null;
                
                int blockEndPos = dos.getLength();
                IndexHelper.IndexInfo cIndexInfo =
                        new IndexHelper.IndexInfo(blockFirstColumn.name(), column.name(), blockStartPos-firstBlockPos, blockWidth, blockEndPos-blockStartPos);
                indexList.add(cIndexInfo);
                indexSizeInBytes += cIndexInfo.serializedSize();
            }
            
            // serialize column index
            BigdataColumnIndexer.serialize(indexList, indexSizeInBytes, headerDos);
        }
        catch (IOException e)
        {
            logger.error(e.toString());
            throw new RuntimeException(e);
        }
        finally
        {
            context.releaseCompressor();
        }
    }
    
    /**
     * !BIGDATA:
     * ************************************************************************
     * New serialized ColumnFamily row with column index.
     * The new  method to write column family row (column index and columns)
     * to SSTable.
     * ************************************************************************
     * 
     * the new format of ColumnFamily row:
     * |------------------------|
     * |     bloom filter       | (int for len, and content)
     * |------------------------|
     * |     deletion meta      | localDeletionTime(int) and markedForDeleteAt(long)
     * |------------------------|
     * |     column count       | (int)
     * |------------------------|<-------|<---- COLUMN BLOCKS START POSITION(CBSP)
     * |     column block 0     |        |      BLOCK-INDEX OFFSET FROM CBSP
     * |      (compressed)      |        |
     * |----------    ----------|<---|   |
     * |     column block 1     |    |   |
     * |      (compressed)      |    |   |
     * |------------------------|<---|---|------|
     * |    column index size   |    |   |      | (byte size of column index)
     * |------------------------|    |   |      |
     * |    index of block 0    |----|----      |
     * |----------    ----------|    |          | 
     * |    index of block 1    |-----          |
     * |------------------------|               |
     * |       index size       |---------------- to seek to position of index
     * |------------------------|
     * 
     * @param columnFamily
     * @param dos
     * @throws IOException
     */
    public void bigdataSerializeWithIndexesAtEnd(ColumnFamily columnFamily, DataOutputBuffer dos, Compression.Algorithm compressAlgo)
    {
        // get the sorted columns from column family row
        Collection<IColumn> columns = columnFamily.getSortedColumns();
        
        // create and serialize bloom filter
        BigdataColumnIndexer.createAndSerializeBloomFiliter(columns, dos);

        /*
         * Maintains a list of Column IndexInfo objects for the columns in this
         * column family row. The key is the column name and the position is the
         * relative offset of that column name from the start of the list.
         * We do this so that we don't read all the columns into memory.
         */
        List<IndexHelper.IndexInfo> indexList = new ArrayList<IndexHelper.IndexInfo>();
        
        // different column family use different compression algorithm.
        CompressionContext context = CompressionContext.getInstance(compressAlgo);
        
        try
        {
            // deletion meta information
            dos.writeInt(columnFamily.localDeletionTime.get());
            dos.writeLong(columnFamily.markedForDeleteAt.get());
            
            // column count
            dos.writeInt(columns.size());

            // the current column
            IColumn column = null;
            // the size of serialized column index, computed up front
            int indexSizeInBytes = 0;
            // the position of first block, at where the column blocks start.
            int firstBlockPos = dos.getLength();
            // the first column of current block
            IColumn blockFirstColumn = null;
            // the start position of current block
            // the column index will store the offset from firstBlockPos
            int blockStartPos = firstBlockPos;
            // uncompressed current block size
            int blockSize = 0;
            // compressed output stream of current block 
            DataOutputStream blockOut = null;
            
            for (Iterator<IColumn> it = columns.iterator(); it.hasNext();)
            {
                column = it.next();

                if ((blockFirstColumn == null) && (blockOut == null))
                {
                    // start a new block
                    blockFirstColumn = column;
                    blockStartPos = dos.getLength();
                    blockSize = 0;
                    // get a new block output stream
                    blockOut = getBlockOutputStream(dos, context);
                }
                
                // serialize column
                columnFamily.getColumnSerializer().serialize(column, blockOut);
                // uncompressed block size
                blockSize += column.serializedSize();
                
                // if we hit the block size that we have to index after, go ahead and index it.
                if (blockSize >= DatabaseDescriptor.getColumnIndexSize())
                {
                    int blockWidth = releaseBlockOutputStream(blockOut, context);
                    assert blockWidth == blockSize;
                    blockOut = null;

                    int blockEndPos = dos.getLength();
                    IndexHelper.IndexInfo cIndexInfo =
                            new IndexHelper.IndexInfo(blockFirstColumn.name(), column.name(), blockStartPos-firstBlockPos, blockWidth, blockEndPos-blockStartPos);
                    indexList.add(cIndexInfo);
                    indexSizeInBytes += cIndexInfo.serializedSize();
                    
                    // to next block
                    blockFirstColumn = null;
                }
            }
            
            if (blockOut != null)
            {
                int blockWidth = releaseBlockOutputStream(blockOut, context);
                assert blockWidth == blockSize;
                blockOut = null;
                
                int blockEndPos = dos.getLength();
                IndexHelper.IndexInfo cIndexInfo =
                        new IndexHelper.IndexInfo(blockFirstColumn.name(), column.name(), blockStartPos-firstBlockPos, blockWidth, blockEndPos-blockStartPos);
                indexList.add(cIndexInfo);
                indexSizeInBytes += cIndexInfo.serializedSize();
            }
            
            // the start position of column index
            int indexStartPos = dos.getLength();
            // serialize column index
            BigdataColumnIndexer.serialize(indexList, indexSizeInBytes, dos);
            // write out the size of index.
            dos.writeInt(dos.getLength() - indexStartPos);
        }
        catch (IOException e)
        {
            logger.error(e.toString());
            throw new RuntimeException(e);
        }
        finally
        {
            context.releaseCompressor();
        }
    }
    
    // BIGDATA: new serializeWithIndexes, to support old and new row format
    public void serializeWithIndexes(ColumnFamily columnFamily, DataOutputBuffer headerDos, DataOutputBuffer dos, Compression.Algorithm compressAlgo)
    {
        if ( (compressAlgo != null) && 
             ( (DatabaseDescriptor.getCompressStartRowSize() <= 0) ||
               columnFamily.isSizeLargeThan(DatabaseDescriptor.getCompressStartRowSize()) ) )
        {
            boolean newIndexAtEnd = DatabaseDescriptor.isNewRowFormatIndexAtEnd();
            // use the id of compressAlgo
            serializeRowFormat(true, newIndexAtEnd, compressAlgo.getId(), headerDos);
            if (newIndexAtEnd)
                bigdataSerializeWithIndexesAtEnd(columnFamily, dos, compressAlgo);
            else
                bigdataSerializeWithIndexes(columnFamily, headerDos, dos, compressAlgo);
        }
        else
        {
            serializeRowFormat(false, false, 0, headerDos);
            oldSerializeWithIndexes(columnFamily, dos);
        }
    }
    
    /**
     * ************************************************************************
     * PAY ATTENTION: This method is only internally used for message.
     * [1] Row-Mutation (RowMutationSerializer)
     * [2] Query-Result (RowSerializer)
     * ************************************************************************
     */
    public ColumnFamily deserialize(DataInput dis) throws IOException
    {
        String cfName = dis.readUTF();
        if (cfName.isEmpty())
            return null;
        ColumnFamily cf = deserializeFromSSTableNoColumns(cfName, dis.readUTF(), readComparator(dis), readComparator(dis), dis);
        deserializeColumns(dis, cf);
        return cf;
    }

    /*
     * ************************************************************************
     * PAY ATTENTION: This method is internally used for message.
     * This is also the old format in SSTable.
     * [1] Row-Mutation (RowMutationSerializer)
     * [2] Query-Result (RowSerializer)
     * ************************************************************************
     */
    private void deserializeColumns(DataInput dis, ColumnFamily cf) throws IOException
    {
        int size = dis.readInt();
        for (int i = 0; i < size; ++i)
        {
            IColumn column = cf.getColumnSerializer().deserialize(dis);
            cf.addColumn(column);
        }
    }

    private AbstractType readComparator(DataInput dis) throws IOException
    {
        String className = dis.readUTF();
        if (className.equals(""))
        {
            return null;
        }

        try
        {
            return (AbstractType)Class.forName(className).getConstructor().newInstance();
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Unable to load comparator class '" + className + "'.  probably this means you have obsolete sstables lying around", e);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public ColumnFamily deserializeFromSSTableNoColumns(String name, String type, AbstractType comparator, AbstractType subComparator, DataInput input) throws IOException
    {
        ColumnFamily cf = new ColumnFamily(name, type, comparator, subComparator);
        return deserializeFromSSTableNoColumns(cf, input);
    }

    public ColumnFamily deserializeFromSSTableNoColumns(ColumnFamily cf, DataInput input) throws IOException
    {
        cf.delete(input.readInt(), input.readLong());
        return cf;
    }

    /**
     * ************************************************************************
     * PAY ATTENTION: This method is internally used for message.
     * This is also the old format in SSTable.
     * [1] Row-Mutation (RowMutationSerializer)
     * [2] Query-Result (RowSerializer)
     * ************************************************************************
     */
    public ColumnFamily deserializeFromSSTable(SSTableReader sstable, DataInput file) throws IOException
    {
        ColumnFamily cf = sstable.makeColumnFamily();
        deserializeFromSSTableNoColumns(cf, file);
        deserializeColumns(file, cf);
        return cf;
    }
    
    
    /*
     * BIGDATA: Get output stream for a column block.
     */
    private DataOutputStream getBlockOutputStream(OutputStream os, CompressionContext context) throws IOException
    {
        context.getCompressor();
        try
        {
            OutputStream cos = context.compressAlgo.createCompressionStream(os, context.compressor, 0);
            return new DataOutputStream(cos);
        }
        catch (IOException e)
        {
            context.releaseCompressor();
            throw e;
        }
    }

    /*
     * BIGDATA: Release block output stream.
     * @return the uncompressed size in the block.
     */
    private int releaseBlockOutputStream(DataOutputStream dos, CompressionContext context) throws IOException
    {
        dos.flush();
        context.releaseCompressor();
        return dos.size();
    }

    /**
     * BIGDATA: Get block input stream
     * @param is
     *            make sure the is's current position is right at the
     *            beginning of data block
     * @param length
     *            the length of block on disk
     */
    public DataInputStream getBlockInputStream(FileDataInput is, int length, CompressionContext context) throws IOException
    {
        context.getDecompressor();
        try
        {
            InputStream bfdis = new BoundedFileDataInputStream(is, length);
            InputStream cis = context.compressAlgo.createDecompressionStream(bfdis, context.decompressor, 0);
            return new DataInputStream(cis);
        }
        catch (IOException e)
        {
            context.releaseDecompressor();
            throw e;
        }
    }

    /**
     * BIGDATA: Release block input stream.
     * @param context
     */
    public void releaseBlockInputStream(InputStream din, CompressionContext context)
    {
        context.releaseDecompressor();
    }
    
    /**
     * BIGDATA: Context for compression.
     */
    public static class CompressionContext
    {
        public final Compression.Algorithm compressAlgo;
        
        public Compressor compressor = null;
        public Decompressor decompressor = null;

        private CompressionContext(Compression.Algorithm compressAlgo)
        {
            this.compressAlgo = compressAlgo;
        }
                
        public static CompressionContext getInstance(Compression.Algorithm compressAlgo)
        {
           return new CompressionContext(compressAlgo);
        }

        public void getCompressor() throws IOException
        {
            assert compressor == null;
            compressor = compressAlgo.getCompressor();
        }
        
        public void getDecompressor() throws IOException
        {
            assert decompressor == null;
            decompressor = compressAlgo.getDecompressor();
        }
        
        public void releaseCompressor()
        {
            if (compressor != null)
            {
                compressAlgo.returnCompressor(compressor);
                compressor = null;
            }
        }
        
        public void releaseDecompressor() 
        {
            if (decompressor != null)
            {
                compressAlgo.returnDecompressor(decompressor);
                decompressor = null;
            }
        }
    }
}
