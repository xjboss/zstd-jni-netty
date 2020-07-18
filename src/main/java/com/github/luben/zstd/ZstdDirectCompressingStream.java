package com.github.luben.zstd;

import com.github.luben.zstd.util.Native;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ZstdDirectCompressingStream implements Closeable, Flushable {

    static {
        Native.load();
    }

    protected final long stream;

    public ZstdDirectCompressingStream(int level) throws IOException {
        synchronized(this) {
            this.level = level;
            stream = createCStream();
        }
    }

    public static int recommendedOutputBufferSize() { return (int)recommendedCOutSize(); }

    protected int consumed = 0;
    protected int produced = 0;
    protected boolean closed = false;
    protected boolean initialized = false;
    protected boolean finalize = true;
    protected int level = 3;
    protected byte[] dict = null;
    protected ZstdDictCompress fastDict = null;

    /* JNI methods */
    protected static native long recommendedCOutSize();
    protected static native long createCStream();
    protected static native int  freeCStream(long ctx);
    protected native int  initCStream(long ctx, int level);
    protected native int  initCStreamWithDict(long ctx, byte[] dict, int dict_size, int level);
    protected native int  initCStreamWithFastDict(long ctx, ZstdDictCompress dict);
    protected native int  compressDirectByteBuffer(long ctx, long dst, int dstOffset, int dstSize, long src, int srcOffset, int srcSize);
    protected native int  flushStream(long ctx, long dst, int dstOffset, int dstSize);
    protected native int  endStream(long ctx, long dst, int dstOffset, int dstSize);

    public synchronized ZstdDirectCompressingStream setDict(byte[] dict) throws IOException {
        if (initialized) {
            throw new IOException("Change of parameter on initialized stream");
        }
        this.dict = dict;
        this.fastDict = null;
        return this;
    }

    public synchronized ZstdDirectCompressingStream setDict(ZstdDictCompress dict) throws IOException {
        if (initialized) {
            throw new IOException("Change of parameter on initialized stream");
        }
        this.dict = null;
        this.fastDict = dict;
        return this;
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

    public synchronized void compress(ByteBuffer source) throws IOException {
        if (!source.isDirect()) {
            throw new IllegalArgumentException("Source buffer should be a direct buffer");
        }
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (!initialized) {
            int result = 0;
            ZstdDictCompress fastDict = this.fastDict;
            if (fastDict != null) {
                fastDict.acquireSharedLock();
                try {
                    result = initCStreamWithFastDict(stream, fastDict);
                } finally {
                    fastDict.releaseSharedLock();
                }
            } else if (dict != null) {
                result = initCStreamWithDict(stream, dict, dict.length, level);
            } else {
                result = initCStream(stream, level);
            }
            if (Zstd.isError(result)) {
                throw new IOException("Compression error: cannot create header: " + Zstd.getErrorName(result));
            }
            initialized = true;
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (closed) {
            throw new IOException("Already closed");
        }
    }

    @Override
    public synchronized void close() throws IOException {
    }

    @Override
    protected void finalize() throws Throwable {
        if (finalize) {
            close();
        }
    }
}
