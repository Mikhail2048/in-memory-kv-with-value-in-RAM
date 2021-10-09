package src.files;

import java.io.File;

public class FileProcessingHelper {

    public String getFileExtension(File file) {
        return file.getName().substring(file.getName().lastIndexOf('.') + 1);
    }
}