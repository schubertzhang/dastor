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


import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.bigdata.dastor.config.DatabaseDescriptor;
import com.bigdata.dastor.db.DecoratedKey;
import com.bigdata.dastor.dht.IPartitioner;
import com.bigdata.dastor.io.util.BufferedRandomAccessFile;
import com.bigdata.dastor.io.util.DataOutputBuffer;
import com.bigdata.dastor.utils.BloomFilter;
import com.bigdata.dastor.utils.FBUtilities;

public class SSTableWriter extends SSTable
{
    private static Logger logger = Logger.getLogger(SSTableWriter.class);

    private BufferedRandomAccessFile dataFile;
    private BufferedRandomAccessFile indexFile;
    private DecoratedKey lastWrittenKey;
    private BloomFilter bf;

    public SSTableWriter(String filename, long keyCount, IPartitioner partitioner) throws IOException
    {
        super(filename, partitioner);
        indexSummary = new IndexSummary();
        dataFile = new BufferedRandomAccessFile(path, "rw", (int)(DatabaseDescriptor.getFlushDataBufferSizeInMB() * 1024 * 1024));
        indexFile = new BufferedRandomAccessFile(indexFilename(), "rw", (int)(DatabaseDescriptor.getFlushIndexBufferSizeInMB() * 1024 * 1024));
        bf = BloomFilter.getFilter(keyCount, 15);
    }

    private long beforeAppend(DecoratedKey decoratedKey) throws IOException
    {
        if (decoratedKey == null)
        {
            throw new IOException("Keys must not be null.");
        }
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) > 0)
        {
            logger.info("Last written key : " + lastWrittenKey);
            logger.info("Current key : " + decoratedKey);
            logger.info("Writing into file " + path);
            throw new IOException("Keys must be written in ascending order.");
        }
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private void afterAppend(DecoratedKey decoratedKey, long dataPosition) throws IOException
    {
        String diskKey = partitioner.convertToDiskFormat(decoratedKey);
        bf.add(diskKey);
        lastWrittenKey = decoratedKey;
        long indexPosition = indexFile.getFilePointer();
        indexFile.writeUTF(diskKey);
        indexFile.writeLong(dataPosition);
        if (logger.isTraceEnabled())
            logger.trace("wrote " + decoratedKey + " at " + dataPosition);
        if (logger.isTraceEnabled())
            logger.trace("wrote index of " + decoratedKey + " at " + indexPosition);

        int rowSize = (int)(dataFile.getFilePointer() - dataPosition);
        indexSummary.maybeAddEntry(decoratedKey, dataPosition, rowSize, indexPosition, indexFile.getFilePointer());
    }

    // TODO make this take a DataOutputStream and wrap the byte[] version to combine them
    /*
    public void append(DecoratedKey decoratedKey, DataOutputBuffer buffer) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        dataFile.writeUTF(partitioner.convertToDiskFormat(decoratedKey));
        int length = buffer.getLength();
        assert length > 0;
        dataFile.writeInt(length);
        dataFile.write(buffer.getData(), 0, length);
        afterAppend(decoratedKey, currentPosition);
    }
    */ // replaced by next one by BIGDATA

    /*
     * BIGDATA: for compression, this method replace the previous one
     */
    public void append(DecoratedKey decoratedKey, DataOutputBuffer headerBuffer, DataOutputBuffer buffer) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        dataFile.writeUTF(partitioner.convertToDiskFormat(decoratedKey));
        int headerBufferLen = headerBuffer.getLength();
        int bufferLen = buffer.getLength();
        int length = headerBufferLen + bufferLen;
        assert length > 0;
        dataFile.writeInt(length);
        if (headerBufferLen > 0)
        {
            dataFile.write(headerBuffer.getData(), 0, headerBufferLen);
        }
        if (bufferLen > 0)
        {
            dataFile.write(buffer.getData(), 0, bufferLen);
        }
        afterAppend(decoratedKey, currentPosition);
    }
    
    public void append(DecoratedKey decoratedKey, byte[] value) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        dataFile.writeUTF(partitioner.convertToDiskFormat(decoratedKey));
        assert value.length > 0;
        dataFile.writeInt(value.length);
        dataFile.write(value);
        afterAppend(decoratedKey, currentPosition);
    }

    /**
     * Renames temporary SSTable files to valid data, index, and bloom filter files
     */
    public SSTableReader closeAndOpenReader() throws IOException
    {
        // bloom filter
        FileOutputStream fos = new FileOutputStream(filterFilename());
        DataOutputStream stream = new DataOutputStream(fos);
        BloomFilter.serializer().serialize(bf, stream);
        stream.flush();
        fos.getFD().sync();
        stream.close();

        // index
        indexFile.getChannel().force(true);
        indexFile.close();

        // main data
        dataFile.close(); // calls force

        rename(indexFilename());
        rename(filterFilename());
        path = rename(path); // important to do this last since index & filter file names are derived from it

        indexSummary.complete();
        return new SSTableReader(path, partitioner, indexSummary, bf);
    }

    static String rename(String tmpFilename)
    {
        String filename = tmpFilename.replace("-" + SSTable.TEMPFILE_MARKER, "");
        try
        {
            FBUtilities.renameWithConfirm(tmpFilename, filename);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return filename;
    }

    public long getFilePointer()
    {
        return dataFile.getFilePointer();
    }
    
    public static SSTableReader renameAndOpen(String dataFileName) throws IOException
    {
        SSTableWriter.rename(indexFilename(dataFileName));
        SSTableWriter.rename(filterFilename(dataFileName));
        dataFileName = SSTableWriter.rename(dataFileName);
        return SSTableReader.open(dataFileName);
    }

}
