package com.bigdata.dastor.io.compress;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

/**
 * Compression wrapper. 
 * 
 * It's a wrapper class of the different compression algorithms. 
 * The users can only use this class and need not concern the detailed 
 * under layer implementations. 
 */
public final class Compression {
  private static final Logger LOG = Logger.getLogger(Compression.class);

  /**
   * Prevent the instantiation of class.
   */
  private Compression() {
    // nothing
  }

  /**
   * The output compression stream which support finish when flush. 
   * The user call flush() to finish current compression stream and flush 
   * compressed data out.
   */
  static class FinishOnFlushCompressionStream extends FilterOutputStream {
    public FinishOnFlushCompressionStream(CompressionOutputStream cout) {
      super(cout);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      CompressionOutputStream cout = (CompressionOutputStream)out;
      cout.finish();
      cout.flush();
      cout.resetState();
    }
  }

  /**
   * Define the compression algorithm names.
   */
  public static final String COMPRESSION_NONE = "none";
  public static final String COMPRESSION_LZO  = "lzo";
  public static final String COMPRESSION_GZ   = "gz";

  public static final String COMPRESSION_NULL = "null"; // just defined here for convenience

  public static final int COMPRESSION_ID_NONE = 0;  
  public static final int COMPRESSION_ID_LZO  = 1;
  public static final int COMPRESSION_ID_GZ   = 2;
  
  /**
   * Compression algorithms.
   */
  public static enum Algorithm {
    LZO(COMPRESSION_LZO, COMPRESSION_ID_LZO) {
      // since LZO is GPL licensed, need load native library
      private static final String DEFAULT_CODEC_CLAZZ =
        "com.hadoop.compression.lzo.LzoCodec";
      private transient boolean checked = false;
      private transient CompressionCodec codec = null;

      @Override
      public synchronized boolean isSupported() {
        if (!checked) {
          checked = true;
          // check hadoop configuration first, then check system properties
          String extClazz = conf.get(CONF_LZO_CLASS);
          if (extClazz == null) {
            extClazz = System.getProperty(CONF_LZO_CLASS);
          }
          String clazz = (extClazz != null) ? extClazz : DEFAULT_CODEC_CLAZZ;
          try {
            LOG.info("Trying to load Lzo codec class: " + clazz);
            codec = (CompressionCodec) ReflectionUtils.
                    newInstance(Class.forName(clazz), conf);
          } catch (ClassNotFoundException e) {
            // that is okay
          }
        }
        return codec != null;
      }

      @Override
      CompressionCodec getCodec() throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "LZO codec class not specified. Did you forget to set property "
              + CONF_LZO_CLASS + "?");
        }

        return codec;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "LZO codec class not specified. Did you forget to set property " 
              + CONF_LZO_CLASS + "?");
        }
        InputStream bis1 = null;
        if (downStreamBufferSize > 0) {
          bis1 = new BufferedInputStream(downStream, downStreamBufferSize);
        } else {
          bis1 = downStream;
        }
        // set the default LZO blocksize (64KB), used in LzoCodec
        conf.setInt("io.compression.codec.lzo.buffersize", 64 * 1024);
        CompressionInputStream cis =
            codec.createInputStream(bis1, decompressor);
        BufferedInputStream bis2 = new BufferedInputStream(cis, DATA_IBUF_SIZE);
        return bis2;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "LZO codec class not specified. Did you forget to set property "
              + CONF_LZO_CLASS + "?");
        }
        OutputStream bos1 = null;
        if (downStreamBufferSize > 0) {
          bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
        } else {
          bos1 = downStream;
        }
        // set the default LZO blocksize (64KB), used in LzoCodec
        conf.setInt("io.compression.codec.lzo.buffersize", 64 * 1024);
        CompressionOutputStream cos =
            codec.createOutputStream(bos1, compressor);
        BufferedOutputStream bos2 =
            new BufferedOutputStream(new FinishOnFlushCompressionStream(cos),
                DATA_OBUF_SIZE);
        return bos2;
      }
    },

    GZ(COMPRESSION_GZ, COMPRESSION_ID_GZ) {
      private transient DefaultCodec codec = null;

      @Override
      synchronized CompressionCodec getCodec() {
        if (codec == null) {
          codec = new DefaultCodec();
          codec.setConf(conf);
        }

        return codec;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        // Set the internal buffer size to read from down stream.
        if (downStreamBufferSize > 0) {
          // used in DefaultCodec
          codec.getConf().setInt("io.file.buffer.size", downStreamBufferSize);
        } else {
          codec.getConf().setInt("io.file.buffer.size", 32 * 1024);
        }
        CompressionInputStream cis =
            codec.createInputStream(downStream, decompressor);
        BufferedInputStream bis2 = new BufferedInputStream(cis, DATA_IBUF_SIZE);
        return bis2;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        OutputStream bos1 = null;
        if (downStreamBufferSize > 0) {
          bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
        } else {
          bos1 = downStream;
        }
        // used in DefaultCodec
        codec.getConf().setInt("io.file.buffer.size", 32 * 1024);
        CompressionOutputStream cos =
            codec.createOutputStream(bos1, compressor);
        BufferedOutputStream bos2 =
            new BufferedOutputStream(new FinishOnFlushCompressionStream(cos),
                DATA_OBUF_SIZE);
        return bos2;
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    },

    NONE(COMPRESSION_NONE, COMPRESSION_ID_NONE) {
      @Override
      CompressionCodec getCodec() {
        return null;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedInputStream(downStream, downStreamBufferSize);
        }
        return downStream;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedOutputStream(downStream, downStreamBufferSize);
        }

        return downStream;
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    };

    // We require that all compression related settings are configured
    // statically in the Configuration object.
    private static final Configuration conf = new Configuration();
    private final String compressName;
    private final int compressId;
    // data input buffer size to absorb small reads from application.
    private static final int DATA_IBUF_SIZE = 1 * 1024;
    // data output buffer size to absorb small writes from application.
    private static final int DATA_OBUF_SIZE = 4 * 1024;
    public static final String CONF_LZO_CLASS =
        "io.compression.codec.lzo.class";

    Algorithm(String name, int id) {
      this.compressName = name;
      this.compressId = id;
    }

    abstract CompressionCodec getCodec() throws IOException;

    public abstract InputStream createDecompressionStream(
        InputStream downStream, Decompressor decompressor,
        int downStreamBufferSize) throws IOException;

    public abstract OutputStream createCompressionStream(
        OutputStream downStream, Compressor compressor,
        int downStreamBufferSize) throws IOException;

    public abstract boolean isSupported();

    public Compressor getCompressor() throws IOException {
      CompressionCodec codec = getCodec();
      if (codec != null) {
        Compressor compressor = CodecPool.getCompressor(codec);
        if (compressor != null) {
          if (compressor.finished()) {
            // Somebody returns the compressor to CodecPool but is still using
            // it.
            LOG.warn("Compressor obtained from CodecPool already finished()");
          } else {
            LOG.debug("Got a compressor: " + compressor.hashCode());
          }
          /**
           * Following statement is necessary to get around bugs in 0.18 where a
           * compressor is referenced after returned back to the codec pool.
           */
          compressor.reset();
        }
        return compressor;
      }
      return null;
    }

    public void returnCompressor(Compressor compressor) {
      if (compressor != null) {
        LOG.debug("Return a compressor: " + compressor.hashCode());
        CodecPool.returnCompressor(compressor);
      }
    }

    public Decompressor getDecompressor() throws IOException {
      CompressionCodec codec = getCodec();
      if (codec != null) {
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        if (decompressor != null) {
          if (decompressor.finished()) {
            // Somebody returns the decompressor to CodecPool but is still using
            // it.
            LOG.warn("Deompressor obtained from CodecPool already finished()");
          } else {
            LOG.debug("Got a decompressor: " + decompressor.hashCode());
          }
          /**
           * Following statement is necessary to get around bugs in 0.18 where a
           * decompressor is referenced after returned back to the codec pool.
           */
          decompressor.reset();
        }
        return decompressor;
      }

      return null;
    }

    public void returnDecompressor(Decompressor decompressor) {
      if (decompressor != null) {
        LOG.debug("Returned a decompressor: " + decompressor.hashCode());
        CodecPool.returnDecompressor(decompressor);
      }
    }

    public String getName() {
      return compressName;
    }
    
    public int getId() {
      return compressId;
    }
  }

  /**
   * Get the compression algorithm by string name
   */
  public static Algorithm getCompressionAlgorithmByName(String compressName) {
    Algorithm[] algos = Algorithm.class.getEnumConstants();

    for (Algorithm a : algos) {
      if (a.getName().equals(compressName)) {
        return a;
      }
    }

    throw new IllegalArgumentException(
        "Unsupported compression algorithm name: " + compressName);
  }

  /**
   * Get the compression algorithm by id
   */
  public static Algorithm getCompressionAlgorithmById(int id) {
    Algorithm[] algos = Algorithm.values();
    if (algos != null) {
      for (Algorithm a : algos) {
        if (a.getId() == id) {
          return a;
        }
      }
    }
    
    throw new IllegalArgumentException(
        "Unsupported compression algorithm id: " + id);
  }
  
  /**
   * List all defined algorithms 
   */
  public static String[] getSupportedAlgorithms() {
    Algorithm[] algos = Algorithm.class.getEnumConstants();

    ArrayList<String> ret = new ArrayList<String>();
    for (Algorithm a : algos) {
      if (a.isSupported()) {
        ret.add(a.getName());
      }
    }
    return ret.toArray(new String[ret.size()]);
  }
}
