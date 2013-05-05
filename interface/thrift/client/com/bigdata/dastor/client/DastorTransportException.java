package com.bigdata.dastor.client;

public class DastorTransportException extends DastorException
{
    private static final long serialVersionUID = 2702183050454612212L;
    
    public DastorTransportException() 
    {
        super();
    }

    public DastorTransportException(String message)
    {
        super(message);
    }

    public DastorTransportException(Throwable cause)
    {
        super(cause);
    }

    public DastorTransportException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
