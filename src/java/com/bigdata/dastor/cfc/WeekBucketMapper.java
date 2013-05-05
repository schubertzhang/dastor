package com.bigdata.dastor.cfc;

public class WeekBucketMapper implements IBucketMapper
{
    private static final String prefix = "BKT";

    @Override
    public String getBucketName(String udBucketName)
    {
        // the udBucketName must like "2010052"
        if (udBucketName.length() < 7)
        {
            return null;
        }
        
        return prefix + udBucketName.substring(4, 7);
    }

}
