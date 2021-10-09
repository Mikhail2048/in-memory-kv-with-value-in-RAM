package src.core.config;

import src.Record;
import src.files.DataFilesProcessingHelper;
import src.files.FileProcessingHelper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryMapPopulator {

    private final FileProcessingHelper fileProcessingHelper;
    private final DataFilesProcessingHelper dataFilesProcessingHelper;

    public InMemoryMapPopulator() {
        this.fileProcessingHelper = new FileProcessingHelper();
        this.dataFilesProcessingHelper = new DataFilesProcessingHelper();
    }

    public ConcurrentMap<String, String> populateLocalCacheWithDataFromDirectory() {
        final String dataDirectoryAbsolutePath = System.getProperty(CacheConfigConstants.DATA_DIRECTORY_LOCATION);
        final File dataDirectory = new File(dataDirectoryAbsolutePath);
        if (dataDirectory.isDirectory()) {
            if (dataDirectory.listFiles() == null) {
                System.out.printf("Found 0 data files in directory %s\n", dataDirectoryAbsolutePath);
                return new ConcurrentHashMap<>();
            } else {
                return createInMemoryMapFromFilesInDirectory(dataDirectory);
            }
        } else {
            throw new RuntimeException(dataDirectoryAbsolutePath + " must point to a directory");
        }
    }

    public ConcurrentMap<String ,String> createInMemoryMapFromFilesInDirectory(File dataDirectory) {
        ConcurrentMap<String, String> completeLocalCacheMap = new ConcurrentHashMap<>();
        Arrays.stream(dataDirectory.listFiles())
                .filter(file -> fileProcessingHelper.getFileExtension(file).equals(System.getProperty(CacheConfigConstants.DATA_FILES_EXTENSION)))
                .sorted(dataFilesProcessingHelper.getDataFilesComparator())
                .map(this::readValuesMapFromFile)
                .forEach(completeLocalCacheMap::putAll);
        System.out.printf("Found %s records in log file\n", completeLocalCacheMap.size());
        return completeLocalCacheMap;
    }

    public ConcurrentMap<String, String> readValuesMapFromFile(File dataFile) {
        ConcurrentMap<String, String> concurrentMap = new ConcurrentHashMap<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(dataFile))) {
            String recordAsString;
            while ((recordAsString = bufferedReader.readLine()) != null) {
                final String[] keyValueArray = recordAsString.split(":");
                final Record record = new Record(keyValueArray[0], keyValueArray[1]);
                concurrentMap.put(record.getKey(), record.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return concurrentMap;
    }
}
