package com.bigdata.dastor.cfc;

public class DayBucketMapper implements IBucketMapper
{
    private static final String prefix = "BKT";
    
    @Override
    public String getBucketName(String udBucketName)
    {
        // the udBucketName must like "20100518"
        if (udBucketName.length() < 8)
        {
            return null;
        }
        
        return prefix + udBucketName.substring(4, 8);
    }

}
