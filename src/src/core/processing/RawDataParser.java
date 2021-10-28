package src.core.processing;

import src.core.models.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RawDataParser {

    private static RawDataParser instance;

    public static RawDataParser getInstance() {
        if (instance == null) {
            instance = new RawDataParser();
        }
        return instance;
    }

    private RawDataParser() {
    }

    public List<Record> parseKeyValuePairs(byte[] rawData) {
        List<Record> records = new ArrayList<>();
        StringBuilder resultParts = new StringBuilder();
        Record parsedRecord = null;
        for (byte rawDataByte : rawData) {
            final char currentSymbol = (char) rawDataByte;
            if (currentSymbol == ':') {
                parsedRecord = new Record(resultParts.toString());
                resultParts.setLength(0);
                continue;
            }
            if (currentSymbol == '\n') {
                Optional.ofNullable(parsedRecord).ifPresent(record -> record.setValue(resultParts.toString()));
                Optional.ofNullable(parsedRecord).ifPresent(records::add);
                resultParts.setLength(0);
                continue;
            }
            resultParts.append(currentSymbol);
        }
        return records;
    }

    public Record parseKeyValuePair(byte[] rawData, int byteOffsetWeAreInterestedIn) {
        Record parsedRecord = null;
        StringBuilder resultParts = new StringBuilder();
        for (int i = byteOffsetWeAreInterestedIn; i < rawData.length; i++) {
            final char currentSymbol = (char) rawData[i];
            if (currentSymbol == ':') {
                parsedRecord = new Record(resultParts.toString());
                resultParts.setLength(0);
                continue;
            }
            if (currentSymbol == '\n') {
                Optional.ofNullable(parsedRecord).ifPresent(record -> record.setValue(resultParts.toString()));
                return parsedRecord;
            }
            resultParts.append(currentSymbol);
        }
        return parsedRecord;
    }

    public String parseValue(byte[] readBytes, int byteOffsetOfTheValueWeAreInterestedIn) {
        int currentByteIndex = byteOffsetOfTheValueWeAreInterestedIn;
        StringBuilder value = new StringBuilder();
        while (readBytes[currentByteIndex] != '\n') {
            value.append((char) readBytes[currentByteIndex]);
            currentByteIndex++;
        }
        return value.toString();
    }
}