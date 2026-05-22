package com.bson.spark.read;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

import com.bson.spark.schema.BsonToRowConverter;

/**
 * Reads documents from a single file partition and converts each
 * to a Spark {@link InternalRow}.
 *
 * <p>Supports two file formats:
 * <ul>
 *   <li><b>BSON</b> (.bson) — binary, concatenated BSON documents</li>
 *   <li><b>EJSON</b> (.json) — line-delimited Extended JSON (MongoDB $out format)</li>
 * </ul>
 */
public final class BsonPartitionReader implements PartitionReader<InternalRow> {

    private final BsonToRowConverter converter;
    private final InputStream bsonInputStream;
    private final BufferedReader jsonReader;
    private final boolean isBsonFormat;

    private InternalRow currentRow;
    private boolean exhausted;

    public BsonPartitionReader(
            BsonPartition partition, BsonToRowConverter converter, Configuration hadoopConf) {
        this.converter = converter;
        this.exhausted = false;

        String filePath = partition.filePath();
        this.isBsonFormat = isBsonFile(filePath);

        InputStream rawStream = openFile(filePath, hadoopConf);
        if (isBsonFormat) {
            this.bsonInputStream = rawStream;
            this.jsonReader = null;
        } else {
            this.bsonInputStream = null;
            this.jsonReader = new BufferedReader(
                    new InputStreamReader(rawStream, StandardCharsets.UTF_8));
        }
    }

    @Override
    public boolean next() throws IOException {
        if (exhausted) {
            return false;
        }

        BsonDocument document;
        if (isBsonFormat) {
            document = readNextBsonDocument();
        } else {
            document = readNextEjsonDocument();
        }

        if (document == null) {
            exhausted = true;
            return false;
        }

        currentRow = converter.convert(document);
        if (currentRow == null) {
            return next();
        }
        return true;
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {
        if (bsonInputStream != null) {
            bsonInputStream.close();
        }
        if (jsonReader != null) {
            jsonReader.close();
        }
    }

    // ─── BSON binary format ────────────────────────────────────────────

    private BsonDocument readNextBsonDocument() throws IOException {
        byte[] sizeBytes = new byte[4];
        int bytesRead = readFully(bsonInputStream, sizeBytes);
        if (bytesRead < 4) {
            return null;
        }

        int docSize = ByteBuffer.wrap(sizeBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (docSize < 5) {
            return null;
        }

        byte[] docBytes = new byte[docSize];
        System.arraycopy(sizeBytes, 0, docBytes, 0, 4);
        int remaining = readFully(bsonInputStream, docBytes, 4, docSize - 4);
        if (remaining < docSize - 4) {
            return null;
        }

        return new RawBsonDocument(docBytes);
    }

    // ─── EJSON (line-delimited JSON) format ────────────────────────────

    private BsonDocument readNextEjsonDocument() throws IOException {
        String line;
        while ((line = jsonReader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            return BsonDocument.parse(line);
        }
        return null;
    }

    // ─── Utilities ─────────────────────────────────────────────────────

    private static boolean isBsonFile(String path) {
        String lower = path.toLowerCase();
        return lower.endsWith(".bson") || lower.endsWith(".bson.gz");
    }

    private static InputStream openFile(String path, Configuration hadoopConf) {
        try {
            Path hadoopPath = new Path(path);
            FileSystem fs = hadoopPath.getFileSystem(hadoopConf);
            return fs.open(hadoopPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open file: " + path, e);
        }
    }

    private static int readFully(InputStream in, byte[] buffer) throws IOException {
        return readFully(in, buffer, 0, buffer.length);
    }

    private static int readFully(InputStream in, byte[] buffer, int offset, int length)
            throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int read = in.read(buffer, offset + totalRead, length - totalRead);
            if (read < 0) {
                break;
            }
            totalRead += read;
        }
        return totalRead;
    }
}
