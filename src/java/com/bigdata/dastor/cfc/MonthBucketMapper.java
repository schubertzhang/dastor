package com.bigdata.dastor.cfc;

public class MonthBucketMapper implements IBucketMapper
{
    private static final String prefix = "BKT";
    
    @Override
    public String getBucketName(String udBucketName)
    {
        // the udBucketName must like "201005"
        if (udBucketName.length() < 6)
        {
            return null;
        }
        
        return prefix + udBucketName.substring(4, 6);
    }
    
}
