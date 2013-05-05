package com.bigdata.dastor.client;

public class Cell
{
    public byte[] name;
    public byte[] value;
    public long timestamp;  // in millisecond
    
    public Cell() {}
    
    public Cell(byte[] name, byte[] value, long timestamp)
    {
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
    }
}
