package com.github.luben.zstd;

import com.github.luben.zstd.util.Native;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ZstdBytebufCompressingStream extends ZstdDirectCompressingStream implements Closeable, Flushable {
    private ByteBuf target;

    /**
     * This method should flush the buffer and either return the same buffer (but cleared) or a new buffer
     * that should be used from then on.
     * @param toFlush buffer that has to be flushed (or most cases, you want to call {@link ByteBuffer#flip()} first)
     * @return the new buffer to use, for most cases the same as the one passed in, after a call to {@link ByteBuffer#clear()}.
     */
    protected ByteBuf flushBuffer(ByteBuf toFlush) throws IOException {
        return toFlush;
    }

    public ZstdBytebufCompressingStream(ByteBuf target, int level) throws IOException {
        super(level);
        if (!target.isDirect()) {
            throw new IllegalArgumentException("Target buffer should be a direct buffer");
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

    public synchronized void compress(ByteBuf source) throws IOException {
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
        while (source.isReadable()) {
            if (!target.isWritable()) {
                target = flushBuffer(target);
                if (!target.isDirect()) {
                    throw new IllegalArgumentException("Target buffer should be a direct buffer");
                }
                if (!target.isWritable()) {
                    throw new IOException("The target buffer has no more space, even after flushing, and there are still bytes to compress");
                }
            }
            int result = compressDirectByteBuffer(stream, target.memoryAddress(), target.writerIndex(), target.writableBytes(), source.memoryAddress(), source.readerIndex(), source.readableBytes());
            if (Zstd.isError(result)) {
                throw new IOException("Compression error: " + Zstd.getErrorName(result));
            }
            target.writerIndex(target.writerIndex() + produced);
            source.readerIndex(source.readerIndex() + consumed);
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (closed) {
            throw new IOException("Already closed");
        }
        if (initialized) {
            int needed;
            do {
                needed = flushStream(stream, target.memoryAddress(), target.writerIndex(), target.capacity());
                if (Zstd.isError(needed)) {
                    throw new IOException("Compression error: " + Zstd.getErrorName(needed));
                }
                target.writerIndex(target.writerIndex() + produced);
                target = flushBuffer(target);
                if (!target.isDirect()) {
                    throw new IllegalArgumentException("Target buffer should be a direct buffer");
                }
                if (needed > 0 && !target.isWritable()) {
                    // don't check on the first iteration of the loop
                    throw new IOException("The target buffer has no more space, even after flushing, and there are still bytes to compress");
                }
            }
            while (needed > 0);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            try {
                if (initialized) {
                    int needed;
                    do {
                        needed = endStream(stream, target.memoryAddress(), target.writerIndex(), target.writableBytes());
                        if (Zstd.isError(needed)) {
                            throw new IOException("Compression error: " + Zstd.getErrorName(needed));
                        }
                        target.writerIndex(target.writerIndex() + produced);
                        target = flushBuffer(target);
                        if (!target.isDirect()) {
                            throw new IllegalArgumentException("Target buffer should be a direct buffer");
                        }
                        if (needed > 0 && !target.isWritable()) {
                            throw new IOException("The target buffer has no more space, even after flushing, and there are still bytes to compress");
                        }
                    } while (needed > 0);
                }
            }
            finally {
                freeCStream(stream);
                closed = true;
                initialized = false;
                target = null; // help GC with realizing the buffer can be released
            }
        }
    }
}
