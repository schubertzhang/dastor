package com.bigdata.dastor.utils;

import java.io.Serializable;

public class DiskSpaceLoad implements Serializable
{
    private static final long serialVersionUID = 8948738115799412166L;
    
    public long net = 0;
    public long gross = 0;
    
    public DiskSpaceLoad() {}
    
    public DiskSpaceLoad(long net, long gross)
    {
        this.net = net;
        this.gross = gross;
    }
}
