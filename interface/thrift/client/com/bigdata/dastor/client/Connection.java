package com.bigdata.dastor.client;

import java.util.List;

public interface Connection
{
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public boolean isClosed();
    public void close();
    
    public void put(String space, String bucket, String key, Cell cell, GuaranteeFactor gFactor)
    throws DastorException;
    
    public void put(String space, String bucket, String key, List<Cell> cellList, GuaranteeFactor gFactor)
    throws DastorException;
    
    public Cell get(String space, String bucket, String key, byte[] cellName, GuaranteeFactor gFactor)
    throws DastorException;

    public List<Cell> get(String space, String bucket, String key, List<byte[]> cellNames, GuaranteeFactor gFactor)
    throws DastorException;

    public List<Cell> get(String space, String bucket, String key, CellRange cellRange, boolean desc, GuaranteeFactor gFactor)
    throws DastorException;
        
    public List<Cell> get(String space, String bucket, String key, boolean desc, GuaranteeFactor gFactor)
    throws DastorException;
}
