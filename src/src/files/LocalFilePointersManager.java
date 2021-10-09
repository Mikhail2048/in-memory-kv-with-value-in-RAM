package src.files;

import src.core.InMemoryHashTablesConstructor;
import src.core.models.DataFilePointer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    private static LocalFilePointersManager instance;

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
    }

    /**
     * Replaces last amount {@param amountOfSquashedMaps} of an in memory maps with the new one.
     * This method will provide an API for {@link src.files.FileSegmentsManager}, where the latter
     * will squash the maps and force {@link LocalFilePointersManager} to update its in-memory state
     *
     * @param amountOfSquashedMaps - amount of in-memory maps at the end of the list (hence, the most stale maps)
     * @param newlyConstructedMap - new Map that have been created by squashing the maps at the end
     * @deprecated because of synchronized block, see {@link #replaceMaps(int, ConcurrentHashMap, long)}
     */
    @Deprecated
    public void replaceMapsSynchronized(int amountOfSquashedMaps, ConcurrentMap<String, Long> newlyConstructedMap, long newDataFileSequenceNumber) {
        synchronized (inMemoryMaps) {
            for (int i = 0; i < amountOfSquashedMaps; i++) {
                inMemoryMaps.removeLast();
            }
            inMemoryMaps.addLast(new DataFilePointer(newDataFileSequenceNumber, newlyConstructedMap));
        }
    }

    /**
     * Replaces last amount {@param amountOfSquashedMaps} of an in memory maps with the new one.
     * This method will provide an API for {@link src.files.FileSegmentsManager}, where the latter
     * will squash the maps and force {@link LocalFilePointersManager} to update its in-memory state
     *
     * @param amountOfSquashedMaps - amount of in-memory maps at the end of the list (hence, the most stale maps)
     * @param newlyConstructedMap - new Map that have been created by squashing the maps at the end
     */
    public void replaceMaps(int amountOfSquashedMaps, ConcurrentHashMap<String, Long> newlyConstructedMap, long newDataFileSequenceNumber) {
        synchronized (inMemoryMaps) {
            for (int i = 0; i < amountOfSquashedMaps; i++) {
                inMemoryMaps.removeLast();
            }
            inMemoryMaps.addLast(new DataFilePointer(newDataFileSequenceNumber, newlyConstructedMap));
        }
    }

    public void putKeyValuePairToLocalPointerMap(String key, Long valueByteOffset) {
        Optional.ofNullable(this.inMemoryMaps.peek()).ifPresentOrElse(
                dataFilePointer -> dataFilePointer.getKeyValueByteOffsetOnDiskMap().put(key, valueByteOffset),
                () -> { throw new IllegalStateException("Writable data file entry is null!"); });
    }

    public String getValueForKey(String key) {
        acquireReadLockWithExpectedValue(0);
        final String value = inMemoryMaps.stream()
                .filter(dataFilePointer -> dataFilePointer.getKeyValueByteOffsetOnDiskMap().containsKey(key))
                .findFirst()
                .map(targetDataFilePointer -> findValueByKeyInTargetFile(key, targetDataFilePointer))
                .orElse(null);
        decrementAmountOfReadRequests();
        return value;
    }

    private void decrementAmountOfReadRequests() {
        FileSegmentsManager.amountOfCurrentReadRequests.decrementAndGet();
    }

    private void acquireReadLockWithExpectedValue(int expectedValueOfReads) {
        final int actualValue = FileSegmentsManager.amountOfCurrentReadRequests.compareAndExchange(expectedValueOfReads, expectedValueOfReads + 1);
        if (actualValue == expectedValueOfReads) return;
        if (actualValue == -1) {
            acquireReadLockWithExpectedValue(expectedValueOfReads);
        } else {
            acquireReadLockWithExpectedValue(actualValue);
        }
    }

    private String findValueByKeyInTargetFile(String key, DataFilePointer targetDataFilePointer) {
        final Path absolutePathForTargetDataFile = dataFilesProcessingHelper.createAbsolutePathForFileWithNumber(targetDataFilePointer.getSequenceNumber());
        try (final RandomAccessFile targetRandomAccessFile = new RandomAccessFile(absolutePathForTargetDataFile.toString(), "r")) {
            targetRandomAccessFile.seek(targetDataFilePointer.getKeyValueByteOffsetOnDiskMap().get(key));
            StringBuilder result = new StringBuilder();
            char readSymbol;
            while ((readSymbol = targetRandomAccessFile.readChar()) != '\n') {
                result.append(readSymbol);
            }
            FileSegmentsManager.amountOfCurrentReadRequests.decrementAndGet();
            return result.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}