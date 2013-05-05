package com.bigdata.dastor.client;

import java.util.Properties;

public class ClientDriver
{
    public static Connection getConnection(String host, int port, Properties props)
    throws DastorException
    {
        return getConnection(host, port, 0, props);
    }
    
    public static Connection getConnection(String host, int port, int timeout, Properties props)
    throws DastorException
    {
        return new ConnectionImpl(host, port, timeout);
    }
}
