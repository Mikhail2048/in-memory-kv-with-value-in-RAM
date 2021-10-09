package src.core.models;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DataFilePointer {

    /**
     * Sequential Number of the Data File. This number is primarily assigned
     * by {@link src.files.FileSegmentsManager}
     */
    private final long sequenceNumber;

    /**
     * This map contains key, as a String key, provided by User to us,
     * and the value is the the byte offset in the file, with sequence number
     * as {@link #sequenceNumber}, where the actual value resides on disk. This
     * Long represents byte offset with the exactly same place, where the client
     * MUST begin reading in order to retrieve the value. In other words, if
     * some hypothetical value resides on disk in some file, beginning at the byte offset
     * 170, then this long (value in the ConcurrentHashMap) will be exactly 170, not 169,
     * not or 171.
     */
    private final ConcurrentMap<String, Long> keyValueByteOffsetOnDiskMap;

    public DataFilePointer(long sequenceNumber) {
        this(sequenceNumber, new ConcurrentHashMap<>());
    }

    public DataFilePointer(long sequenceNumber, ConcurrentMap<String, Long> inMemoryMap) {
        this.sequenceNumber = sequenceNumber;
        this.keyValueByteOffsetOnDiskMap = inMemoryMap;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public ConcurrentMap<String, Long> getKeyValueByteOffsetOnDiskMap() {
        return keyValueByteOffsetOnDiskMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFilePointer that = (DataFilePointer) o;
        return sequenceNumber == that.sequenceNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequenceNumber);
    }
}