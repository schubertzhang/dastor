package com.bigdata.dastor.cfc;

public interface IBucketMapper
{
    // get the internal bucket name from the user-defined bucket name.
    String getBucketName(String udBucketName);
}
