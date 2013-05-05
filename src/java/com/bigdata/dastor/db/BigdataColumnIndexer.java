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

import java.io.IOException;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Iterator;


import com.bigdata.dastor.config.DatabaseDescriptor;
import com.bigdata.dastor.db.marshal.AbstractType;
import com.bigdata.dastor.io.IndexHelper;
import com.bigdata.dastor.io.util.DataOutputBuffer;
import com.bigdata.dastor.utils.BloomFilter;


/**
 * Help to create an index for a column family based on size of columns
 */

public class BigdataColumnIndexer
{
    /**
     * Write column index.
     * The in-memory structure of column index is already built up outside.
     * @param indexList in-memory structure of column index
     * @param dos data output stream
     * @throws IOException
     */
    public static void serialize(List<IndexHelper.IndexInfo> indexList, int indexSizeInBytes, DataOutput dos)
    {
        if (indexSizeInBytes == 0)
        {
            assert (indexList.size() == 0);
        }
        
        try
        {
            dos.writeInt(indexSizeInBytes);
            for (IndexHelper.IndexInfo cIndexInfo : indexList)
            {
                cIndexInfo.serialize(dos);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Create a bloom filter that contains the subcolumns and the columns that
     * make up this Column Family.
     * @param columns columns of the ColumnFamily
     * @return BloomFilter with the summarized information.
     */
    private static BloomFilter createColumnBloomFilter(Collection<IColumn> columns)
    {
        int columnCount = 0;
        for (IColumn column : columns)
        {
            columnCount += column.getObjectCount();
        }

        BloomFilter bf = BloomFilter.getFilter(columnCount, 4);
        for (IColumn column : columns)
        {
            bf.add(column.name());
            /* If this is SuperColumn type Column Family we need to get the subColumns too. */
            if (column instanceof SuperColumn)
            {
                Collection<IColumn> subColumns = column.getSubColumns();
                for (IColumn subColumn : subColumns)
                {
                    bf.add(subColumn.name());
                }
            }
        }
        return bf;
    }

    /**
     * Create a bloom filter and write it.
     * @param columns columns of the ColumnFamily
     * @param dos data output stream
     * @throws IOException
     */
    public static void createAndSerializeBloomFiliter(Collection<IColumn> columns, DataOutput dos)
    {
        BloomFilter bf = createColumnBloomFilter(columns);        
        /* Write out the bloom filter. */
        DataOutputBuffer bufOut = new DataOutputBuffer();
        try
        {
            BloomFilter.serializer().serialize(bf, bufOut);
            /* write the length of the serialized bloom filter. */
            dos.writeInt(bufOut.getLength());
            /* write out the serialized bytes. */
            dos.write(bufOut.getData(), 0, bufOut.getLength());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
