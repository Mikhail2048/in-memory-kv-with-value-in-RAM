package src.core.models;

public class DataFileScanOptions {
    private final Long dataFileSequenceNumber;
    private Long fromByteOffset;
    private Long toByteOffset;

    public DataFileScanOptions(Long dataFileSequenceNumber) {
        this.dataFileSequenceNumber = dataFileSequenceNumber;
    }

    public DataFileScanOptions(Long dataFileSequenceNumber, Long fromByteOffset, Long toByteOffset) {
        this(dataFileSequenceNumber);
        this.fromByteOffset = fromByteOffset;
        this.toByteOffset = toByteOffset;
    }

    public void setFromByteOffset(Long fromByteOffset) {
        this.fromByteOffset = fromByteOffset;
    }

    public void setToByteOffset(Long toByteOffset) {
        this.toByteOffset = toByteOffset;
    }

    public Long getDataFileSequenceNumber() {
        return dataFileSequenceNumber;
    }

    public Long getFromByteOffset() {
        return fromByteOffset;
    }

    public Long getToByteOffset() {
        return toByteOffset;
    }
}