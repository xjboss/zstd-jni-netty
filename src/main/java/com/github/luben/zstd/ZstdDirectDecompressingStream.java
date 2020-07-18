package com.github.luben.zstd;

import com.github.luben.zstd.util.Native;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ZstdDirectDecompressingStream implements Closeable {

    static {
        Native.load();
    }

    protected final long stream;
    protected boolean finishedFrame = false;
    protected boolean closed = false;
    protected boolean streamEnd = false;
    protected boolean finalize = true;

    protected static native int recommendedDOutSize();
    protected static native long createDStream();
    protected static native int  freeDStream(long stream);
    protected native int initDStream(long stream);
    protected native long decompressStream(long stream, long dst, int dstOffset, int dstSize, long src, int srcOffset, int srcSize);

    public ZstdDirectDecompressingStream() {
        synchronized(this) {
            //this.source = source;
            stream = createDStream();
            initDStream(stream);
        }
    }

    /**
     * Enable or disable class finalizers
     *
     * If finalizers are disabled the responsibility fir calling the `close` method is on the consumer.
     *
     * @param finalize, default `true` - finalizers are enabled
     */
    public void setFinalize(boolean finalize) {
        this.finalize = finalize;
    }

    public static int recommendedTargetBufferSize() {
        return (int) recommendedDOutSize();
    }

    public synchronized ZstdDirectDecompressingStream setDict(byte[] dict) throws IOException {
        int size = Zstd.loadDictDecompress(stream, dict, dict.length);
        if (Zstd.isError(size)) {
            throw new IOException("Decompression error: " + Zstd.getErrorName(size));
        }
        return this;
    }

    public synchronized ZstdDirectDecompressingStream setDict(ZstdDictDecompress dict) throws IOException {
        dict.acquireSharedLock();
        try {
            int size = Zstd.loadFastDictDecompress(stream, dict);
            if (Zstd.isError(size)) {
                throw new IOException("Decompression error: " + Zstd.getErrorName(size));
            }
        } finally {
            dict.releaseSharedLock();
        }
        return this;
    }

    protected int consumed;
    protected int produced;


    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            try {
                freeDStream(stream);
            }
            finally {
                closed = true;
                //source = null; // help GC with realizing the buffer can be released
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (finalize) {
            close();
        }
    }
}
