package com.bigdata.dastor.db;

import java.net.UnknownHostException;

public interface HintedHandOffManagerMBean
{
    /**
     * @return the number of hints (rows) had been delivered
     */
    public long getDeliveredRows();

    /**
     * @return the current delivering endpoint
     */
    public String getDeliveringEp();

    /**
     * @return the current delivering application table
     */
    public String getDeliveringTable();

    /**
     * @return the current delivering application cf
     */
    public String getDeliveringCf();

    /**
     * @param to the endpoint to send hints to
     * @throws UnknownHostException
     */
    public void deliverHints(String to) throws UnknownHostException;
}
