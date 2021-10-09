package src.files;

import src.core.config.CacheConfigConstants;
import src.files.exception.FileInvalidFormatException;

import java.io.File;
import java.util.Comparator;

public class DataFilesProcessingHelper {

    private FileProcessingHelper fileProcessingHelper = new FileProcessingHelper();

    public boolean isDataFile(File file) {
        return fileProcessingHelper.getFileExtension(file).equals(System.getProperty(CacheConfigConstants.DATA_FILES_EXTENSION));
    }

    public long getDataFileSequenceNumber(File dataFile) throws FileInvalidFormatException {
        final int start = dataFile.getName().lastIndexOf('-') + 1;
        final int end = dataFile.getName().lastIndexOf('.');
        final String fileSequenceNumber = dataFile.getName().substring(start, end);
        try {
            return Integer.parseInt(fileSequenceNumber);
        } catch (NumberFormatException e) {
            throw new FileInvalidFormatException();
        }
    }

    public Comparator<File> getDataFilesComparator() {
        return (firstFile, secondFile) -> {
            try {
                return getDataFileSequenceNumber(firstFile) > getDataFileSequenceNumber(secondFile) ? 1 : -1;
            } catch (FileInvalidFormatException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
