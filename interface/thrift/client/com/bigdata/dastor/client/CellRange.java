package com.bigdata.dastor.client;

public class CellRange
{
    public byte[] startCellName;
    public byte[] endCellName;
    public int limitCount;
    
    public CellRange() {};
    
    public CellRange(byte[] startCellName, byte[] endCellName, int limitCount)
    {
        this.startCellName = startCellName;
        this.endCellName = endCellName;
        this.limitCount = limitCount;
    }
}
