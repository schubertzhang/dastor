package com.bigdata.dastor.io.util;

import java.io.IOException;
import java.io.InputStream;

public class BoundedFileDataInputStream extends InputStream {
    private final FileDataInput in;
    private final long start;
    private final long end;    
    private final byte[] oneByte = new byte[1];

    public BoundedFileDataInputStream(FileDataInput fdi, long width) {
        if (width < 0) {
            throw new IndexOutOfBoundsException("Invalid length: " + width);
        }

        this.in = fdi;
        this.start = fdi.getAbsolutePosition();
        this.end = this.start + width;
    }

    @Override
    public int read() throws IOException {
        int ret = read(oneByte);
        if (ret == 1)
            return oneByte[0] & 0xff;
        return -1;
    }

    @Override
    public int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }
        int n = (int)Math.min(Integer.MAX_VALUE, Math.min(len, end-in.getAbsolutePosition()));
        if (n == 0)
            return -1;
        in.readFully(b, off, n);
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        long len = Math.min(n, end-in.getAbsolutePosition());
        int actualSkip = in.skipBytes((int)len);
        assert actualSkip == len;
        return len;
    }

    @Override
    public int available() throws IOException {
        return (int)(end - in.getAbsolutePosition());
    }

    @Override
    public void close() throws IOException {
    }
}
