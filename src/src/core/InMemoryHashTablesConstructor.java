package src.core;

import src.core.config.CacheConfigConstants;
import src.core.models.DataFilePointer;
import src.files.DataFilesProcessingHelper;
import src.files.FileProcessingHelper;
import src.files.exception.FileInvalidFormatException;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InMemoryHashTablesConstructor {

    private final FileProcessingHelper fileProcessingHelper;
    private final DataFilesProcessingHelper dataFilesProcessingHelper;

    public InMemoryHashTablesConstructor() {
        this.fileProcessingHelper = new FileProcessingHelper();
        this.dataFilesProcessingHelper = new DataFilesProcessingHelper();
    }

    public LinkedList<DataFilePointer> constructDataFilesPointers() {
        final String dataDirectoryAbsolutePath = System.getProperty(CacheConfigConstants.DATA_DIRECTORY_LOCATION);
        final File dataDirectory = new File(dataDirectoryAbsolutePath);
        if (dataDirectory.isDirectory()) {
            if (dataDirectory.listFiles() == null || dataDirectory.listFiles().length == 0) {
                System.out.printf("Found 0 data files in directory %s\n", dataDirectoryAbsolutePath);
                return new LinkedList<>();
            } else {
                return createInMemoryMapFromFilesInDirectory(dataDirectory);
            }
        } else {
            throw new RuntimeException(dataDirectoryAbsolutePath + " must point to a directory");
        }
    }

    private LinkedList<DataFilePointer> createInMemoryMapFromFilesInDirectory(File dataDirectory) {
        return Arrays.stream(dataDirectory.listFiles())
                .filter(file -> fileProcessingHelper.getFileExtension(file).equals(System.getProperty(CacheConfigConstants.DATA_FILES_EXTENSION)))
                .sorted(dataFilesProcessingHelper.getDataFilesComparatorOldComesLast())
                .map(this::readValuesMapFromFile)
                .collect(Collectors.toCollection(LinkedList::new));
    }

    public DataFilePointer readValuesMapFromFile(File dataFile) {
        ConcurrentHashMap<String, Long> keyBytesOffsetPointersMap = new ConcurrentHashMap<>();
        try {
            final DataInputStream dataInputStream = new DataInputStream(new FileInputStream(dataFile));
            char readSymbol;
            StringBuilder key = new StringBuilder();
            long valueByteOffset = 0L;
            while (dataInputStream.available() != 0) {
                while ((readSymbol = dataInputStream.readChar()) != ':') {
                    key.append(readSymbol);
                    valueByteOffset += 2; // Because char is 2 bytes long
                }
                valueByteOffset += 3; // Because we read ':' character, but did not count it. Also the value itself starts not by ':', but ty the next byte
                keyBytesOffsetPointersMap.put(key.toString(), valueByteOffset);
                key.setLength(0); // Clear the StringBuilder
                while ((readSymbol = dataInputStream.readChar()) != '\n') {
                    valueByteOffset += 2;
                }
                valueByteOffset += 2;
            }
            return new DataFilePointer(dataFilesProcessingHelper.getDataFileSequenceNumber(dataFile), keyBytesOffsetPointersMap);
        } catch (IOException | FileInvalidFormatException e) {
            e.printStackTrace();
            throw new InternalError();
        }
    }
}
