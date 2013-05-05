package com.bigdata.dastor.client;

public class DastorException extends Exception
{
    private static final long serialVersionUID = 1880615422626340232L;
    
    public DastorException()
    {
        super();
    }

    public DastorException(String message)
    {
        super(message);
    }

    public DastorException(Throwable cause)
    {
        super(cause);
    }

    public DastorException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
