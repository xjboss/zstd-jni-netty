package com.github.luben.zstd;

import com.github.luben.zstd.util.Native;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;

public class ZstdBytebufDecompressingStream extends ZstdDirectDecompressingStream implements Closeable {
    /**
     * Override this method in case the byte buffer passed to the constructor might not contain the full compressed stream
     * @param toRefill current buffer
     * @return either the current buffer (but refilled and flipped if there was new content) or a new buffer.
     */
    protected ByteBuf refill(ByteBuf toRefill) {
        return toRefill;
    }
    private ByteBuf source;

    public ZstdBytebufDecompressingStream(ByteBuf source) {
        super();
        if (!source.isDirect()) {
            throw new IllegalArgumentException("Source buffer should be a direct buffer");
        }
        synchronized(this) {
            this.source = source;
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

    public synchronized boolean hasRemaining() {
        return !streamEnd && (source.isReadable() || !finishedFrame);
    }

    public static int recommendedTargetBufferSize() {
        return (int) recommendedDOutSize();
    }

    public synchronized ZstdBytebufDecompressingStream setDict(byte[] dict) throws IOException {
        int size = Zstd.loadDictDecompress(stream, dict, dict.length);
        if (Zstd.isError(size)) {
            throw new IOException("Decompression error: " + Zstd.getErrorName(size));
        }
        return this;
    }

    public synchronized ZstdBytebufDecompressingStream setDict(ZstdDictDecompress dict) throws IOException {
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
    public synchronized int read(ByteBuf target) throws IOException {
        if (!target.isDirect()) {
            throw new IllegalArgumentException("Target buffer should be a direct buffer");
        }
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (streamEnd) {
            return 0;
        }

        long remaining = decompressStream(stream, target.memoryAddress(), target.writerIndex(), target.writableBytes(), source.memoryAddress(), source.readerIndex(), source.readableBytes());
        if (Zstd.isError(remaining)) {
            throw new IOException(Zstd.getErrorName(remaining));
        }

        source.readerIndex(source.readerIndex() + consumed);
        target.writerIndex(target.writerIndex() + produced);

        if (!source.isReadable()) {
            source = refill(source);
            if (!source.isDirect()) {
                throw new IllegalArgumentException("Source buffer should be a direct buffer");
            }
        }

        finishedFrame = remaining == 0;
        if (finishedFrame) {
            // nothing left, so at end of the stream
            streamEnd = !source.isReadable();
        }

        return produced;
    }


    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            try {
                freeDStream(stream);
            }
            finally {
                closed = true;
                source = null; // help GC with realizing the buffer can be released
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
