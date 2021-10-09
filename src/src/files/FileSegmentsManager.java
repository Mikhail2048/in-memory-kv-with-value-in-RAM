package src.files;

import src.core.config.CacheConfigConstants;
import src.core.config.InMemoryMapPopulator;
import src.files.exception.FileInvalidFormatException;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class FileSegmentsManager {

    private final DataFilesProcessingHelper dataFilesProcessingHelper;
    private final InMemoryMapPopulator inMemoryMapPopulator;
    final String dataDirectoryLocation = System.getProperty(CacheConfigConstants.DATA_DIRECTORY_LOCATION);

    public FileSegmentsManager() {
        this.inMemoryMapPopulator = new InMemoryMapPopulator();
        this.dataFilesProcessingHelper = new DataFilesProcessingHelper();
    }

    public void triggerWatcherThread() {
        final Thread thread = new Thread(this::manageDirectoryContent);
        thread.setDaemon(true);
        thread.start();
    }

    private void manageDirectoryContent() {
        while (true) {
            final File dataDirectory = new File(dataDirectoryLocation);
            final File[] dataFiles = dataDirectory.listFiles(getDataFilesFilter());
            if (dataFiles != null) {
                squashDataFilesIfNecessary(dataFiles);
                archiveCurrentLogFileIfNecessary();
            }
        }
    }

    private FilenameFilter getDataFilesFilter() {
        return (dir, name) -> dataFilesProcessingHelper.isDataFile(new File(dir.getAbsoluteFile() + File.separator + name));
    }

    private void squashDataFilesIfNecessary(File[] dataFiles) {
        if (dataFiles.length > Integer.parseInt(System.getProperty(CacheConfigConstants.MAX_DATA_FILES_AMOUNT))) {
            squashDataFilesTail(dataFiles);
        }
    }

    private void squashDataFilesTail(File[] dataFiles) {
        final List<File> tailDataFiles = getTailDataFiles(dataFiles);
        ConcurrentMap<String, String> squashedFileContentMap = new ConcurrentHashMap<>();
        tailDataFiles.stream().map(inMemoryMapPopulator::readValuesMapFromFile).forEach(squashedFileContentMap::putAll);
        try {
            final Path squashedFile = createNewLogFileWithSeqNumber(dataDirectoryLocation, 0);
            populateNewSquashedFile(squashedFileContentMap, squashedFile);
            deleteTailFiles(tailDataFiles);
            squashedFile.toFile().renameTo(new File(tailDataFiles.get(0).getAbsolutePath()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteTailFiles(List<File> tailDataFiles) {
        tailDataFiles.forEach(file -> {
            try {
                Files.deleteIfExists(file.toPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void populateNewSquashedFile(ConcurrentMap<String, String> squashedFileContentMap, Path squashedFile) {
        squashedFileContentMap.forEach((key, value) -> {
            try {
                Files.write(
                        squashedFile,
                        (key + ":" + value + "\n").getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.APPEND
                );
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private List<File> getTailDataFiles(File[] dataFiles) {
        return Arrays.stream(dataFiles)
                .sorted(dataFilesProcessingHelper.getDataFilesComparator())
                .limit(5)
                .collect(Collectors.toList());
    }

    private void archiveCurrentLogFileIfNecessary() {
        String currentLogFileName = getCurrentLogFileOrCreateInitialLogFileIfAbsent(dataDirectoryLocation);
        final File currentLogFile = new File(dataDirectoryLocation + File.separator + currentLogFileName);
        try {
            checkCurrentLogFileSizeAndCreateNewIfRequired(dataDirectoryLocation, currentLogFile);
        } catch (IOException | FileInvalidFormatException e) {
            e.printStackTrace();
        }
    }

    private String getCurrentLogFileOrCreateInitialLogFileIfAbsent(String dataDirectoryLocation) {
        final String currentLogFileName = determineCurrentLogFile();
        if (currentLogFileName == null) {
            try {
                final Path initialLogFile = createNewLogFileWithSeqNumber(dataDirectoryLocation, 1);
                final String fileName = initialLogFile.getFileName().toString();
                System.out.println("Creating initial log file with name " + fileName);
                System.setProperty(CacheConfigConstants.CURRENT_LOG_FILE_NAME, fileName);
                return fileName;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return currentLogFileName;
    }

    private String determineCurrentLogFile() {
        final String currentLogFile = System.getProperty(CacheConfigConstants.CURRENT_LOG_FILE_NAME);
        if (currentLogFile != null) return currentLogFile;
        final File[] files = new File(dataDirectoryLocation).listFiles(getDataFilesFilter());
        if (files != null && files.length != 0) {
            final List<File> sortedLogFiles = Arrays.stream(files).sorted(dataFilesProcessingHelper.getDataFilesComparator()).collect(Collectors.toList());
            final String writableLogFile = sortedLogFiles.get(sortedLogFiles.size() - 1).getName();
            System.setProperty(CacheConfigConstants.CURRENT_LOG_FILE_NAME, writableLogFile);
            return writableLogFile;
        } else {
            return null;
        }
    }

    private void checkCurrentLogFileSizeAndCreateNewIfRequired(String dataDirectoryLocation, File currentLogFile)
            throws IOException, FileInvalidFormatException {
        final long sizeInBytes = Files.size(currentLogFile.toPath());
        final long sizeInKilobytes = sizeInBytes / 1024;
        if (sizeInKilobytes >= Integer.parseInt(System.getProperty(CacheConfigConstants.DATA_FILES_MAX_SIZE_IN_KILOBYTES))) {
            switchToNewLogFile(dataDirectoryLocation, currentLogFile);
        }
    }

    private void switchToNewLogFile(String dataDirectoryLocation, File currentLogFile) throws FileInvalidFormatException, IOException {
        long currentLogFileSeqNumber = dataFilesProcessingHelper.getDataFileSequenceNumber(currentLogFile);
        final Path newLogFile = createNewLogFileWithSeqNumber(dataDirectoryLocation, currentLogFileSeqNumber + 1);
        System.out.printf("Archiving : '%s', Creating : '%s'\n", currentLogFile.getName(), newLogFile.getFileName().toString());
        System.setProperty(CacheConfigConstants.CURRENT_LOG_FILE_NAME, newLogFile.getFileName().toString());
    }

    private Path createNewLogFileWithSeqNumber(String dataDirectoryLocation, long currentLogFileSeqNumber) throws IOException {
        return Files.createFile(Paths.get(URI.create("file:///" + dataDirectoryLocation + File.separator + CacheConfigConstants.DATA_FILE_NAME_PREFIX + "-" + currentLogFileSeqNumber + "." + System.getProperty(CacheConfigConstants.DATA_FILES_EXTENSION))));
    }
}
