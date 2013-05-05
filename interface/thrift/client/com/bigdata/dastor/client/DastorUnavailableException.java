package com.bigdata.dastor.client;

public class DastorUnavailableException extends DastorException
{
    private static final long serialVersionUID = -5142358437169789854L;

    public DastorUnavailableException()
    {
        super();
    }

    public DastorUnavailableException(String message)
    {
        super(message);
    }

    public DastorUnavailableException(Throwable cause)
    {
        super(cause);
    }

    public DastorUnavailableException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
