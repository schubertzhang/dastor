package com.bigdata.dastor.client;

public class DastorInvalidRequestException extends DastorException
{
    private static final long serialVersionUID = -4041633667432715555L;

    public DastorInvalidRequestException()
    {
        super();
    }

    public DastorInvalidRequestException(String message)
    {
        super(message);
    }

    public DastorInvalidRequestException(Throwable cause)
    {
        super(cause);
    }

    public DastorInvalidRequestException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
