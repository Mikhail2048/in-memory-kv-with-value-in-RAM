package src.files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RealValuesFileReader {

    public ConcurrentMap<String, String> readKeyValuePairs(File sourceDataFile) {
        ConcurrentMap<String, String> keyValueMap = new ConcurrentHashMap<>();
        try (final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(sourceDataFile)))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] parts = line.split(":");
                keyValueMap.put(parts[0], parts[1]);
            }
            return keyValueMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}