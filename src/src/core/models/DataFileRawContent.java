package src.core.models;

public class DataFileRawContent {

    private final Long dataFileSequenceNumber;

    public void setRawData(String rawData) {
        this.rawData = rawData;
    }

    private String rawData;

    /**
     * The byte offset, beginning from which {@link #rawData} was read
     */
    private final Long rawDataByteOffset;

    public DataFileRawContent(Long dataFileSequenceNumber, String rawData, Long rawDataByteOffset) {
        this.dataFileSequenceNumber = dataFileSequenceNumber;
        this.rawData = rawData;
        this.rawDataByteOffset = rawDataByteOffset;
    }

    public Long getDataFileSequenceNumber() {
        return dataFileSequenceNumber;
    }

    public String getRawData() {
        return rawData;
    }

    public Long getRawDataByteOffset() {
        return rawDataByteOffset;
    }
}