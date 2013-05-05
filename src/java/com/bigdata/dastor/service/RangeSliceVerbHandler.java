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

package com.bigdata.dastor.service;

import org.apache.log4j.Logger;

import com.bigdata.dastor.db.ColumnFamilyStore;
import com.bigdata.dastor.db.RangeSliceCommand;
import com.bigdata.dastor.db.RangeSliceReply;
import com.bigdata.dastor.db.Table;
import com.bigdata.dastor.net.IVerbHandler;
import com.bigdata.dastor.net.Message;
import com.bigdata.dastor.net.MessagingService;

public class RangeSliceVerbHandler implements IVerbHandler
{

    private static final Logger logger = Logger.getLogger(RangeSliceVerbHandler.class);

    public void doVerb(Message message)
    {
        try
        {
            RangeSliceCommand command = RangeSliceCommand.read(message);
            ColumnFamilyStore cfs = Table.open(command.keyspace).getColumnFamilyStore(command.column_family);
            RangeSliceReply reply = cfs.getRangeSlice(command.super_column,
                                                      command.range,
                                                      command.max_keys,
                                                      command.predicate.slice_range,
                                                      command.predicate.column_names);
            Message response = reply.getReply(message);
            if (logger.isDebugEnabled())
                logger.debug("Sending " + reply+ " to " + message.getMessageId() + "@" + message.getFrom());
            MessagingService.instance.sendOneWay(response, message.getFrom());
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
