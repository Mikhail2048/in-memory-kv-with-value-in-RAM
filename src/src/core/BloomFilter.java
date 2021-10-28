package src.core;

import java.util.Objects;

/**
 * Extremely simple implementation of Bloom Filter data structure
 */
public class BloomFilter {

    private final byte[] hashesArray;
    private static final int DEFAULT_ARRAY_SIZE = Short.MAX_VALUE;
    private static final String SALT = "SALT123";

    public BloomFilter(int length) {
        hashesArray = new byte[length];
    }

    public BloomFilter() {
        this(DEFAULT_ARRAY_SIZE);
    }

    public boolean mayExists(String key) {
        final StringBuilder keyToHash = new StringBuilder(key);
        for (int i = 0; i < 3; i++) {
            final int index = calculateIndex(keyToHash.toString());
            if (hashesArray[index] != 1) {
                return false;
            }
            keyToHash.append(SALT);
        }
        return true;
    }

    public void put(String key) {
        final StringBuilder keyToHash = new StringBuilder(key);
        for (int i = 0; i < 3; i++) {
            hashesArray[calculateIndex(keyToHash.toString())] = 1;
            keyToHash.append(SALT);
        }
    }

    private int calculateIndex(String keyToHash) {
        return Math.abs(Objects.hash(keyToHash) % hashesArray.length);
    }
}