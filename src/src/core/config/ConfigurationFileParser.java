package src.core.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class ConfigurationFileParser {

    private final String CONFIG_FILE_NAME = "aisa.properties";
    private final String CONFIGURATION_FILE_PATH = "/home/misha/designing_data_instensive_applications/3/" + CONFIG_FILE_NAME;

    public void parseConfiguration() throws IOException {
        try (final InputStreamReader reader = new InputStreamReader(new FileInputStream(CONFIGURATION_FILE_PATH))) {
            final Properties properties = new Properties();
            properties.load(reader);
            logReadProperties(properties);
            System.setProperties(properties);
        }
    }

    private void logReadProperties(Properties properties) {
        System.out.println("----------------------------------------Read Properties----------------------------------------");
        System.out.println(CacheConfigConstants.DATA_FILES_EXTENSION + " : " + properties.getProperty(CacheConfigConstants.DATA_FILES_EXTENSION));
        System.out.println(CacheConfigConstants.DATA_DIRECTORY_LOCATION + " : " + properties.getProperty(CacheConfigConstants.DATA_DIRECTORY_LOCATION));
        System.out.println(CacheConfigConstants.MAX_DATA_FILES_AMOUNT + " : " + properties.getProperty(CacheConfigConstants.MAX_DATA_FILES_AMOUNT));
        System.out.println(CacheConfigConstants.DATA_FILES_MAX_SIZE_IN_KILOBYTES + " : " + properties.getProperty(CacheConfigConstants.DATA_FILES_MAX_SIZE_IN_KILOBYTES));
        System.out.println("----------------------------------------End----------------------------------------");
    }
}