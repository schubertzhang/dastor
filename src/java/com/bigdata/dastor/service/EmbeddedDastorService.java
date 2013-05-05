package com.bigdata.dastor.service;
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


import java.io.IOException;

import com.bigdata.dastor.thrift.server.DastorMain;
import org.apache.thrift.transport.TTransportException;

/**
 * An embedded, in-memory dastor storage service that listens
 * on the thrift interface as configured in storage-conf.xml
 * This kind of service is useful when running unit tests of
 * services using dastor for example.
 *
 * How to use:
 * In the client code create a new thread and spawn it with its {@link Thread#start()} method.
 * Example:
 * <pre>
 *      // Tell dastor where the configuration files are.
        System.setProperty("system-config", "conf");

        dastor = new EmbeddedDastorService();
        dastor.init();

        // spawn dastor in a new thread
        Thread t = new Thread(dastor);
        t.setDaemon(true);
        t.start();

 * </pre>
 * @author Ran Tavory (rantav@gmail.com)
 *
 */
public class EmbeddedDastorService implements Runnable
{

    DastorMain dastorMain;

    public void init() throws TTransportException, IOException
    {
        dastorMain = new DastorMain();
        dastorMain.init(null);
    }

    public void run()
    {
        dastorMain.start();
    }
}
