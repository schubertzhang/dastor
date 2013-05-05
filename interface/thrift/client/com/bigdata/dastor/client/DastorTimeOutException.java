package com.bigdata.dastor.client;

public class DastorTimeOutException extends DastorException
{
    private static final long serialVersionUID = 5972957120691926079L;

    public DastorTimeOutException() 
    {
        super();
    }

    public DastorTimeOutException(String message)
    {
        super(message);
    }

    public DastorTimeOutException(Throwable cause)
    {
        super(cause);
    }

    public DastorTimeOutException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
