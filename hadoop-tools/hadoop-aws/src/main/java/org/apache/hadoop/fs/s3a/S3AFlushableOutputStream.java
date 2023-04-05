package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.Abortable;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Statistic.INVOCATION_ABORT;

public class S3AFlushableOutputStream extends OutputStream implements
        StreamCapabilities, IOStatisticsSource, Syncable, Abortable {
    public static final Logger LOG = LoggerFactory.getLogger(S3AFlushableOutputStream.class);

    // TODO initialize to have an initial size of fs.s3a.multipart.size
    private final List<Byte> bytes;

    private final S3ABlockOutputStream.BlockOutputStreamBuilder builder;

    private AtomicBoolean closed;

    S3AFlushableOutputStream(S3ABlockOutputStream.BlockOutputStreamBuilder builder)
            throws IOException {
        this.closed = new AtomicBoolean(false);
        this.builder = builder;
        this.bytes = Collections.synchronizedList(new ArrayList<>((int) DEFAULT_MULTIPART_SIZE));
        touch();
    }

    @Override
    public void write(int b) throws IOException {
        if (!this.closed.get())
            bytes.add((byte)b);
    }

    @Override
    public AbortableResult abort() {
        return null;
    }

    /**
     * Return the stream capabilities.
     * This stream always returns false when queried about hflush and hsync.
     * If asked about {@link CommitConstants#STREAM_CAPABILITY_MAGIC_OUTPUT}
     * it will return true iff this is an active "magic" output stream.
     * @param capability string to query the stream support for.
     * @return true if the capability is supported by this instance.
     */
    @SuppressWarnings("deprecation")
    @Override
    public boolean hasCapability(String capability) {
        switch (capability.toLowerCase(Locale.ENGLISH)) {

            // does the output stream have delayed visibility
            case CommitConstants.STREAM_CAPABILITY_MAGIC_OUTPUT:
            case CommitConstants.STREAM_CAPABILITY_MAGIC_OUTPUT_OLD:
                return true;

            // The flush/sync options are absolutely not supported
            case StreamCapabilities.HFLUSH:
            case StreamCapabilities.HSYNC:
                return false;

            // yes, we do statistics.
            case StreamCapabilities.IOSTATISTICS:
                return true;

            // S3A supports abort.
            case StreamCapabilities.ABORTABLE_STREAM:
                return true;

            default:
                return false;
        }
    }

    /**
     * Write an empty object to S3 that we can later overwrite. Needed for Accumulo WALs to stay in sync with ZK
     */
    private void touch() throws IOException {
        new S3ABlockOutputStream(builder).close();
    }

    @Override
    public synchronized void flush() throws IOException {

        S3ABlockOutputStream outputStream = new S3ABlockOutputStream(builder);
        int len = bytes.size();
        byte[] data = new byte[len];
        for(int i = 0; i < len; i++) {
            data[i] = bytes.get(i);
        }
        outputStream.write(data, 0, bytes.size());
        outputStream.close();
    }

    @Override
    public void hflush() throws IOException {
        flush();
    }

    @Override
    public void hsync() throws IOException {
        hflush();
    }

    @Override
    public void close() throws IOException {
        this.closed = new AtomicBoolean(true);
        flush();
    }
}
