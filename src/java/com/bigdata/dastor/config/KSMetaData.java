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

package com.bigdata.dastor.config;


import com.bigdata.dastor.cfc.IBucketMapper;
import com.bigdata.dastor.locator.AbstractReplicationStrategy;
import com.bigdata.dastor.locator.IEndPointSnitch;
import com.bigdata.dastor.utils.FBUtilities;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public final class KSMetaData
{
    public final String name;
    public final Class<? extends AbstractReplicationStrategy> repStratClass;
    public final int replicationFactor;
    public final IEndPointSnitch epSnitch;
    public final Map<String, CFMetaData> cfMetaData = new HashMap<String, CFMetaData>();

    // BIGDATA:
    public final IBucketMapper bucketMapper;

    KSMetaData(String name, Class<? extends AbstractReplicationStrategy> repStratClass, int replicationFactor, IEndPointSnitch epSnitch,
               IBucketMapper bucketMapper)
    {
        this.name = name;
        this.repStratClass = repStratClass;
        this.replicationFactor = replicationFactor;
        this.epSnitch = epSnitch;
        this.bucketMapper = bucketMapper; // BIGDATA
    }
    
    public boolean equals(Object obj)
    {
        if (obj == null)
            return false;
        if (!(obj instanceof KSMetaData))
            return false;
        KSMetaData other = (KSMetaData)obj;
        return other.name.equals(name)
                && FBUtilities.equals(other.repStratClass, repStratClass)
                && other.replicationFactor == replicationFactor
                && sameEpSnitch(other, this)
                && sameBucketMapper(other, this) // BIGDATA
                && other.cfMetaData.size() == cfMetaData.size()
                && other.cfMetaData.equals(cfMetaData);
    }

    // epsnitches generally have no state, so comparing class names is sufficient.
    private static boolean sameEpSnitch(KSMetaData a, KSMetaData b)
    {
        if (a.epSnitch == null && b.epSnitch == null)
            return true;
        else if (a.epSnitch == null && b.epSnitch != null)
            return false;
        else if (a.epSnitch != null && b.epSnitch == null)
            return false;
        else
            return a.epSnitch.getClass().getName().equals(b.epSnitch.getClass().getName());
    }

    // BIGDATA:
    // compare class name of BucketMapper
    private static boolean sameBucketMapper(KSMetaData a, KSMetaData b)
    {
        if (a.bucketMapper == null && b.bucketMapper == null)
            return true;
        else if (a.bucketMapper == null && b.bucketMapper != null)
            return false;
        else if (a.bucketMapper != null && b.bucketMapper == null)
            return false;
        else
            return a.bucketMapper.getClass().getName().equals(b.bucketMapper.getClass().getName());
    }

    // BIGDATA:
    public static byte[] serialize(KSMetaData ksm) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeUTF(ksm.name);
        dout.writeBoolean(ksm.repStratClass != null);
        if (ksm.repStratClass != null)
            dout.writeUTF(ksm.repStratClass.getName());
        dout.writeInt(ksm.replicationFactor);
        dout.writeBoolean(ksm.epSnitch != null);
        if (ksm.epSnitch != null)
            dout.writeUTF(ksm.epSnitch.getClass().getName());
        dout.writeBoolean(ksm.bucketMapper != null);
        if (ksm.bucketMapper != null)
            dout.writeUTF(ksm.bucketMapper.getClass().getName());
        dout.writeInt(ksm.cfMetaData.size());
        for (CFMetaData cfm : ksm.cfMetaData.values())
            dout.write(CFMetaData.serialize(cfm));
        dout.close();
        return bout.toByteArray();
    }

    // BIGDATA:
    public static KSMetaData deserialize(InputStream in) throws IOException
    {
        DataInputStream din = new DataInputStream(in);
        String name = din.readUTF();
        Class<AbstractReplicationStrategy> repStratClass = null;
        try
        {
            repStratClass = din.readBoolean() ? (Class<AbstractReplicationStrategy>)Class.forName(din.readUTF()) : null;
        }
        catch (Exception ex)
        {
            throw new IOException(ex);
        }
        int replicationFactor = din.readInt();
        IEndPointSnitch epSnitch = null;
        try
        {
            epSnitch = din.readBoolean() ? (IEndPointSnitch)Class.forName(din.readUTF()).newInstance() : null;
        }
        catch (Exception ex)
        {
            throw new IOException(ex);
        }
        IBucketMapper bucketMapper = null;
        try
        {
            bucketMapper = din.readBoolean() ? (IBucketMapper)Class.forName(din.readUTF()).newInstance() : null;
        }
        catch (Exception ex)
        {
            throw new IOException(ex);
        }
        int cfsz = din.readInt();
        KSMetaData ksm = new KSMetaData(name, repStratClass, replicationFactor, epSnitch, bucketMapper);
        for (int i = 0; i < cfsz; i++)
        {
            try
            {
                CFMetaData cfm = CFMetaData.deserialize(din);
                ksm.cfMetaData.put(cfm.cfName, cfm);
            }
            catch (IOException ex)
            {
                System.err.println(ksm.name);
                throw ex;
            }
        }
        return ksm;
    }
}
