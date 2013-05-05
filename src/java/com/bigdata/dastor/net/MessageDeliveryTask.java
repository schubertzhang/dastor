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

package com.bigdata.dastor.net;

import org.apache.log4j.Logger;

import com.bigdata.dastor.config.DatabaseDescriptor;
import com.bigdata.dastor.service.StorageService;

public class MessageDeliveryTask implements Runnable
{
    private static final Logger logger_ = Logger.getLogger(MessageDeliveryTask.class);    

    private Message message_;
    private final long constructionTime_ = System.currentTimeMillis();

    public MessageDeliveryTask(Message message)
    {
        message_ = message;    
    }
    
    public void run()
    {
        StorageService.Verb verb = message_.getVerb();
        switch (verb)
        {
            case BINARY:
            case MUTATION:
            case READ:
            case RANGE_SLICE:
            case READ_REPAIR:
                if (System.currentTimeMillis() > (constructionTime_ + (2*DatabaseDescriptor.getRpcTimeout())))
                {
                    MessagingService.incrementDroppedMessages(verb);
                    return;
                }
                break;
            default:
                break;
        }

        IVerbHandler verbHandler = MessagingService.instance.getVerbHandler(verb);
        assert verbHandler != null : "unknown verb " + verb;
        verbHandler.doVerb(message_);
    }
}
