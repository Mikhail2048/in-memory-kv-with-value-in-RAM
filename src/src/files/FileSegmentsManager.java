package src.files;

import src.core.config.CacheConfigConstants;
import src.core.InMemoryHashTablesConstructor;
import src.core.models.DataFilePointer;
import src.files.exception.FileInvalidFormatException;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class FileSegmentsManager {

    /**
     * if value:
     * > 0 - this means that some threads are still reading data
     * = 0  this means, that there is no threads reading, AND FileSegmentsManager is NOT yet acquired decremented the value
     * < 0 - this means, that no threads are reading, and moreover no threads are capable of reading
     */
    public static volatile AtomicInteger amountOfCurrentReadRequests = new AtomicInteger(0);
    private final DataFilesProcessingHelper dataFilesProcessingHelper;
    private final InMemoryHashTablesConstructor inMemoryHashTablesConstructor;
    private final LocalFilePointersManager localFilePointersManager;
    private final RealValuesFileReader realValuesFileReader;

    final String dataDirectoryLocation = System.getProperty(CacheConfigConstants.DATA_DIRECTORY_LOCATION);

    public FileSegmentsManager() {
        this.inMemoryHashTablesConstructor = new InMemoryHashTablesConstructor();
        this.dataFilesProcessingHelper = new DataFilesProcessingHelper();
        this.localFilePointersManager = LocalFilePointersManager.getInstance();
        realValuesFileReader = new RealValuesFileReader();
    }

    public void triggerWatcherThread() {
        final Thread elderSegmentsSquasherThread = new Thread(this::triggerSquashElderSegmentsProcess);
        final Thread writableFileSegmentSplitterThread = new Thread(this::triggerArchiveCurrentLogFileProcess);
        elderSegmentsSquasherThread.setDaemon(true);
        elderSegmentsSquasherThread.start();
        writableFileSegmentSplitterThread.setDaemon(true);
        writableFileSegmentSplitterThread.start();
    }

    private void triggerSquashElderSegmentsProcess() {
        while (true) {
            final File dataDirectory = new File(dataDirectoryLocation);
            final File[] dataFiles = dataDirectory.listFiles(getDataFilesFilter());
            if (dataFiles != null) {
                squashDataFilesIfNecessary(dataFiles);
            }
        }
    }

    private void triggerArchiveCurrentLogFileProcess() {
        while (true) {
            archiveCurrentLogFileIfNecessary();
        }
    }

    private FilenameFilter getDataFilesFilter() {
        return (dir, name) -> dataFilesProcessingHelper.isDataFile(new File(dir.getAbsoluteFile() + File.separator + name));
    }

    private void squashDataFilesIfNecessary(File[] dataFiles) {
        if (isAmountOfDataFilesExceededTopThreshold(dataFiles)) {
            squashDataFilesTail(dataFiles);
        }
    }

    private boolean isAmountOfDataFilesExceededTopThreshold(File[] dataFiles) {
        return dataFiles.length > Integer.parseInt(System.getProperty(CacheConfigConstants.MAX_DATA_FILES_AMOUNT));
    }

    private void squashDataFilesTail(File[] dataFiles) {
        final List<File> tailDataFiles = getTailDataFiles(dataFiles);
        ConcurrentMap<String, String> squashedFileContentMap = new ConcurrentHashMap<>();
        tailDataFiles.stream().map(realValuesFileReader::readKeyValuePairs).forEach(squashedFileContentMap::putAll);
        try {
            final Path newSquashedFile = createNewLogFileWithSeqNumber(0);
            populateNewSquashedFileWithContentFromMap(squashedFileContentMap, newSquashedFile);
            cleanUp(inMemoryHashTablesConstructor.readValuesMapFromFile(newSquashedFile.toFile()).getKeyValueByteOffsetOnDiskMap(), tailDataFiles, newSquashedFile);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (FileInvalidFormatException e) {
            System.out.printf("%s : Invalid file format! Exception message : %s", e.getCause(), e.getMessage());
        }
    }

    private void cleanUp(ConcurrentMap<String, Long> squashedFileContentMap, List<File> tailDataFiles, Path squashedTemporaryFile) throws FileInvalidFormatException {
        deleteTailFiles(tailDataFiles);
        final File newDataFile = new File(tailDataFiles.get(0).getAbsolutePath());
        squashedTemporaryFile.toFile().renameTo(newDataFile);
        localFilePointersManager.replaceMapsSynchronized(
                tailDataFiles.size(),
                squashedFileContentMap,
                dataFilesProcessingHelper.getDataFileSequenceNumber(newDataFile)
        );
    }

    private void deleteTailFiles(List<File> tailDataFiles) {
        acquireLockForDeletion();
        tailDataFiles.forEach(file -> {
            try {
                Files.deleteIfExists(file.toPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        releaseLock();
    }

    private void releaseLock() {
        amountOfCurrentReadRequests.compareAndSet(-1, 0);
    }

    private void acquireLockForDeletion() {
        boolean lockAcquiredSuccessfully;
        do {
            lockAcquiredSuccessfully = amountOfCurrentReadRequests.compareAndSet(0, -1);
        } while (!lockAcquiredSuccessfully);
    }

    private void populateNewSquashedFileWithContentFromMap(ConcurrentMap<String, String> squashedFileContentMap, Path squashedFile) {
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
                .sorted(dataFilesProcessingHelper.getDataFilesComparatorOldComesFirst())
                .limit(5)
                .collect(Collectors.toList());
    }

    private void archiveCurrentLogFileIfNecessary() {
        String currentLogFileName = getCurrentLogFileOrCreateInitialLogFileIfAbsent();
        final File currentLogFile = new File(dataDirectoryLocation + File.separator + currentLogFileName);
        try {
            checkCurrentLogFileSizeAndCreateNewIfRequired(currentLogFile);
        } catch (IOException | FileInvalidFormatException e) {
            e.printStackTrace();
        }
    }

    private String getCurrentLogFileOrCreateInitialLogFileIfAbsent() {
        final String currentLogFileName = getCurrentLogFileName();
        if (currentLogFileName == null) {
            try {
                return createNewLogFileAndReturnName();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return currentLogFileName;
    }

    private String createNewLogFileAndReturnName() throws IOException {
        final Path initialLogFile = createNewLogFileWithSeqNumber(1);
        localFilePointersManager.addWritableDataFile(new DataFilePointer(1));
        final String fileName = initialLogFile.getFileName().toString();
        System.out.println("Creating initial log file with name " + fileName);
        System.setProperty(CacheConfigConstants.CURRENT_LOG_FILE_NAME, fileName);
        return fileName;
    }

    private String getCurrentLogFileName() {
        return System.getProperty(CacheConfigConstants.CURRENT_LOG_FILE_NAME) != null
                ? System.getProperty(CacheConfigConstants.CURRENT_LOG_FILE_NAME)
                : tryToDetermineCurrentFile();
    }

    private String tryToDetermineCurrentFile() {
        final File[] files = new File(dataDirectoryLocation).listFiles(getDataFilesFilter());
        if (files != null && files.length != 0) {
            final List<File> sortedLogFiles = Arrays.stream(files).sorted(dataFilesProcessingHelper.getDataFilesComparatorOldComesFirst()).collect(Collectors.toList());
            final String writableLogFile = sortedLogFiles.get(sortedLogFiles.size() - 1).getName();
            System.setProperty(CacheConfigConstants.CURRENT_LOG_FILE_NAME, writableLogFile);
            return writableLogFile;
        } else {
            return null;
        }
    }

    private void checkCurrentLogFileSizeAndCreateNewIfRequired(File currentLogFile)
            throws IOException, FileInvalidFormatException {
        final long sizeInBytes = Files.size(currentLogFile.toPath());
        final long sizeInKilobytes = sizeInBytes / 1024;
        if (sizeInKilobytes >= Integer.parseInt(System.getProperty(CacheConfigConstants.DATA_FILES_MAX_SIZE_IN_KILOBYTES))) {
            switchToNewLogFile(currentLogFile);
        }
    }

    private void switchToNewLogFile(File currentLogFile) throws FileInvalidFormatException, IOException {
        long currentLogFileSeqNumber = dataFilesProcessingHelper.getDataFileSequenceNumber(currentLogFile);
        final Path newLogFile = createNewLogFileWithSeqNumber( currentLogFileSeqNumber + 1);
        System.out.printf("Archiving : '%s', Creating : '%s'\n", currentLogFile.getName(), newLogFile.getFileName().toString());
        System.setProperty(CacheConfigConstants.CURRENT_LOG_FILE_NAME, newLogFile.getFileName().toString());
    }

    private Path createNewLogFileWithSeqNumber(long currentLogFileSeqNumber) throws IOException {
        return Files.createFile(dataFilesProcessingHelper.createAbsolutePathForFileWithNumber(currentLogFileSeqNumber));
    }
}
