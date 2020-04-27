package util;

public class CSVItem {
    int key;
    String hash;

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    CSVItem(int key, String hash){
        this.key=key;
        this.hash=hash;
    }
}
