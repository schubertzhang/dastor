/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.bigdata.dastor.service;

import com.bigdata.dastor.thrift.InvalidRequestException;

public interface StorageProxyMBean
{
    public long getReadOperations();
    public long getTotalReadLatencyMicros();
    public double getRecentReadLatencyMicros();    
    // BIGDATA
    public double getRecentReadLatencyMs();
    public double getRecentReadThroughput();

    public long getRangeOperations();
    public long getTotalRangeLatencyMicros();
    public double getRecentRangeLatencyMicros();
    // BIGDATA
    public double getRecentRangeLatencyMs();
    public double getRecentRangeThroughput();

    public long getWriteOperations();
    public long getTotalWriteLatencyMicros();
    public double getRecentWriteLatencyMicros();
    // BIGDATA
    public double getRecentWriteLatencyMs();
    public double getRecentWriteThroughput();

    public boolean getHintedHandoffEnabled();
    public void setHintedHandoffEnabled(boolean b);
    
    // BIGDATA
    public void graceResetClusterCF(String tableName, String bucketName) throws InvalidRequestException; 
    public void graceResetClusterCFUndo(String tableName, String bucketName) throws InvalidRequestException; 
}
