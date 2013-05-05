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

import java.io.*;

import java.net.InetAddress;
import java.nio.ByteBuffer;


import org.apache.log4j.Logger;


import com.bigdata.dastor.net.*;
import com.bigdata.dastor.utils.FBUtilities;

public class RowMutationVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(RowMutationVerbHandler.class);

    public void doVerb(Message message)
    {
        byte[] bytes = message.getMessageBody();
        ByteArrayInputStream buffer = new ByteArrayInputStream(bytes);

        try
        {
            RowMutation rm = RowMutation.serializer().deserialize(new DataInputStream(buffer));
            if (logger_.isDebugEnabled())
              logger_.debug("Applying " + rm);

            /* Check if there were any hints in this message */
            byte[] hintedBytes = message.getHeader(RowMutation.HINT);
            if (hintedBytes != null)
            {
                assert hintedBytes.length > 0;
                ByteBuffer bb = ByteBuffer.wrap(hintedBytes);
                byte[] addressBytes = new byte[FBUtilities.getLocalAddress().getAddress().length];
                while (bb.remaining() > 0)
                {
                    bb.get(addressBytes);
                    InetAddress hint = InetAddress.getByAddress(addressBytes);
                    if (logger_.isDebugEnabled())
                        logger_.debug("Adding hint for " + hint);
                    
                    // BIGDATA: the schema of hinted hand-off changed
                    for(ColumnFamily cf : rm.getColumnFamilies())
                    {
                        // hinted row key: EP+Table+CF,joined by "-" ,Standard column: real rowKey.
                        RowMutation hintedMutation = new RowMutation(Table.SYSTEM_TABLE, HintedHandOffManager.makeHintKey(hint.getHostAddress(), rm.getTable(), cf.name()));
                        hintedMutation.addHints(rm.key());
                        hintedMutation.apply();
                    }
                }
            }

            // BIGDATA:
            Table tab = Table.open(rm.getTable());
            
            // BIGDATA: check if the related column-families are writable.
            if (!isAllCFWritable(rm, tab))
            {
                // silently quit, let client get timeout.
                return;
            }
            
            tab.apply(rm, bytes, true);

            WriteResponse response = new WriteResponse(rm.getTable(), rm.key(), true);
            Message responseMessage = WriteResponse.makeWriteResponseMessage(message, response);
            if (logger_.isDebugEnabled())
              logger_.debug(rm + " applied.  Sending response to " + message.getMessageId() + "@" + message.getFrom());
            MessagingService.instance.sendOneWay(responseMessage, message.getFrom());
        }
        catch (IOException e)
        {
            logger_.error("Error in row mutation", e);
        }
    }
    
    // BIGDATA: check if the related column-families are writable.
    private boolean isAllCFWritable(RowMutation rm, Table tab) throws IOException
    {
        for (String cfName : rm.columnFamilyNames())
        {
            ColumnFamilyStore cfs = tab.getColumnFamilyStore(cfName);
            if (cfs.getStatus() != ColumnFamilyStore.CF_STATUS_NORMAL)
            {
                if (logger_.isInfoEnabled())
                    logger_.info(rm + " ingnored. Since CF " + cfName + " is not in normal status!");
                return false;
            }
        }
        return true;
    }
}
