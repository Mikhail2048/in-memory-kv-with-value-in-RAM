package src.core.models;

import java.util.Objects;

public class Record implements Comparable<Record> {

    private String key;
    private String value;

    public Record(String key) {
        this.key = key;
    }

    public Record(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return Objects.equals(key, record.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public int compareTo(Record o) {
        return this.key.compareTo(o.getKey());
    }

    @Override
    public String toString() {
        return "Record{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
