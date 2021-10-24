package src.files;

import src.core.InMemoryHashTablesConstructor;
import src.core.config.CacheConfigConstants;
import src.core.models.DataFilePointer;
import src.core.models.Record;
import src.files.exception.FileInvalidFormatException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is responsible to manage local in-memory hash tables
 */
public class LocalFilePointersManager {

    /**
     * Firstly comes more fresh data files, and lastly comes more old data files
     * So, therefore, the current writeable file represented here as the first
     * element in the LinkedList
     */
    private final LinkedList<DataFilePointer> inMemoryMaps;
    private final DataFilesProcessingHelper dataFilesProcessingHelper;
    private final InMemoryHashTablesConstructor inMemoryHashTablesConstructor;
    private final NavigableMap<String, String> memoryRedBlackTree;
    private static LocalFilePointersManager instance;

    private final Object writeLock = new Object();

    /**
     * TODO: Make thread safe.
     */
    public static LocalFilePointersManager getInstance() {
        if (instance == null) {
            return (instance = new LocalFilePointersManager());
        }
        return instance;
    }

    private LocalFilePointersManager() {
        this.dataFilesProcessingHelper = new DataFilesProcessingHelper();
        this.inMemoryHashTablesConstructor = new InMemoryHashTablesConstructor();
        this.inMemoryMaps = inMemoryHashTablesConstructor.constructDataFilesPointers();
        this.memoryRedBlackTree = Collections.synchronizedNavigableMap(new TreeMap<>()); //TODO: find out a better approach then synchronization
    }

    /**
     * Replaces last amount {@param amountOfSquashedMaps} of an in memory maps with the new one.
     * This method will provide an API for {@link src.files.FileSegmentsManager}, where the latter
     * will squash the maps and force {@link LocalFilePointersManager} to update its in-memory state
     *
     * @param amountOfSquashedMaps - amount of in-memory maps at the end of the list (hence, the most stale maps)
     * @param newlyConstructedRedBlackTree - new Map that have been created by squashing the maps at the end
     */
    public void replaceMapsSynchronized(int amountOfSquashedMaps, NavigableMap<String, Long> newlyConstructedRedBlackTree, long newDataFileSequenceNumber) {
        synchronized (inMemoryMaps) {
            for (int i = 0; i < amountOfSquashedMaps; i++) {
                inMemoryMaps.removeLast();
            }
            inMemoryMaps.addLast(new DataFilePointer(newDataFileSequenceNumber, newlyConstructedRedBlackTree));
        }
    }

    public void putAllIntoInMemoryTree(Collection<Record> records) {
        records.forEach(record -> memoryRedBlackTree.put(record.getKey(), record.getValue()));
    }

    public void putKeyValuePairToLocalTree(String key, String value) throws IOException {
        doubleCheckLockingOnInMemoryRBTSize();
        final Path temporaryLogFileLocation = Paths.get(System.getProperty(CacheConfigConstants.TEMPORARY_LOG_LOCATION));
        Files.write(
                temporaryLogFileLocation,
                (key + ":" + value + "\n").getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.APPEND
        );
        this.memoryRedBlackTree.put(key, value);
    }

    private void doubleCheckLockingOnInMemoryRBTSize() {
        if (memoryRedBlackTree.size() >= 5000) {
            synchronized (writeLock) {
                if (memoryRedBlackTree.size() >= 5000) {
                    flushInMemoryRedBlackTreeToDisk();
                }
            }
        }
    }

    private void flushInMemoryRedBlackTreeToDisk() {
        try (final FileChannel temporaryLogFileChannel = FileChannel.open(Path.of(System.getProperty(CacheConfigConstants.TEMPORARY_LOG_LOCATION)), StandardOpenOption.WRITE);
             final FileLock logFileLock = temporaryLogFileChannel.lock()) {
            persistCurrentInMemoryRBTToSSTableOnDisk();
        } catch (FileInvalidFormatException | IOException e) {
            e.printStackTrace();
        }
    }

    private void persistCurrentInMemoryRBTToSSTableOnDisk() throws FileInvalidFormatException, IOException {
        final DataFilePointer currentWritableSSTableFilePointer = new DataFilePointer(
                dataFilesProcessingHelper.getDataFileSequenceNumber(new File(System.getProperty(CacheConfigConstants.CURRENT_WRITABLE_SS_TABLE_FILE_LOCATION)))
        );
        final NavigableMap<String, Long> archivedTreeMap = new TreeMap<>();
        StringBuilder dataToBeFlushedToDisk = new StringBuilder();
        AtomicLong valueByteOffset = new AtomicLong();
        memoryRedBlackTree.entrySet()// <-- here we traverse the RBT, and the order is ascending
                .stream()
                .peek(keyValueEntry -> dataToBeFlushedToDisk.append(keyValueEntry.getKey()).append(":").append(keyValueEntry.getValue()).append("\n"))
                .forEach(keyValueEntry -> {
                    final long currentValueByteOffset = valueByteOffset.addAndGet((keyValueEntry.getKey() + ":").getBytes(StandardCharsets.UTF_8).length + 1);
                    archivedTreeMap.put(keyValueEntry.getKey(), currentValueByteOffset);
                    valueByteOffset.addAndGet((keyValueEntry.getValue() + "\n").getBytes(StandardCharsets.UTF_8).length);
                });
        writePassedDataToCurrentSSTable(dataToBeFlushedToDisk);
        currentWritableSSTableFilePointer.setKeyValueByteOffsetOnDiskRedBlackTree(archivedTreeMap);
        inMemoryMaps.addFirst(currentWritableSSTableFilePointer);
        memoryRedBlackTree.clear();
        FileSegmentsManager.getInstance().truncateLogFile();
    }

    private void writePassedDataToCurrentSSTable(StringBuilder dataToBeFlushedToDisk) throws IOException {
        Files.write(
                Path.of(System.getProperty(CacheConfigConstants.CURRENT_WRITABLE_SS_TABLE_FILE_LOCATION)),
                dataToBeFlushedToDisk.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
        );
    }

    public String getValueForKey(String key) {
        String value;
        if ((value = memoryRedBlackTree.get(key)) == null) {
            value = findValueInArchivedSegments(key);
        }
        return value;
    }

    private String findValueInArchivedSegments(String key) {
        acquireReadLockWithExpectedValue(0);
        final String value = inMemoryMaps.stream()
                .filter(dataFilePointer -> dataFilePointer.getKeyValueByteOffsetOnDiskRedBlackTree().containsKey(key))
                .findFirst()
                .map(targetDataFilePointer -> findValueByKeyInTargetFile(key, targetDataFilePointer))
                .orElse(null);
        decrementAmountOfReadRequests();
        return value;
    }

    private void decrementAmountOfReadRequests() {
        System.out.println("Decremented read lock to : " + FileSegmentsManager.amountOfCurrentReadRequests.decrementAndGet());
    }

    private void acquireReadLockWithExpectedValue(int expectedValueOfReads) {
        final int actualValue = FileSegmentsManager.amountOfCurrentReadRequests.compareAndExchange(expectedValueOfReads, expectedValueOfReads + 1);
        if (actualValue == expectedValueOfReads) {
            System.out.println("Read lock acquired, current lock value : " + (expectedValueOfReads + 1));
            return;
        }
        if (actualValue == -1) {
            acquireReadLockWithExpectedValue(expectedValueOfReads);
            System.out.println("Unable to acquire read lock, actual value : -1");
        } else {
            System.out.println("Unable to acquire read lock, actual value : " + actualValue);
            acquireReadLockWithExpectedValue(actualValue);
        }
    }

    private String findValueByKeyInTargetFile(String key, DataFilePointer targetDataFilePointer) {
        final Path absolutePathForTargetDataFile = dataFilesProcessingHelper.createAbsolutePathForFileWithNumber(targetDataFilePointer.getSequenceNumber());
        try (final RandomAccessFile targetRandomAccessFile = new RandomAccessFile(absolutePathForTargetDataFile.toString(), "r")) {
            targetRandomAccessFile.seek(targetDataFilePointer.getKeyValueByteOffsetOnDiskRedBlackTree().get(key));
            StringBuilder result = new StringBuilder();
            char readSymbol;
            while ((readSymbol = ((char) targetRandomAccessFile.read())) != '\n') {
                result.append(readSymbol);
            }
            return result.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}