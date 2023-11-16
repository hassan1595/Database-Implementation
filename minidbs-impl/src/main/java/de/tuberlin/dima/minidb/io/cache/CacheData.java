package de.tuberlin.dima.minidb.io.cache;

public class CacheData {

    private final CacheableData page;
    private final int resourceId;

    private int pinNumber;
    private boolean eviction;
    private boolean prefetched;

    public CacheData(int resourceId, CacheableData page) {
        this.resourceId = resourceId;
        this.page = page;
        this.pinNumber = 0;
        this.eviction = false;
        this.prefetched = true;
    }

    public int getPinNumber() {
        return pinNumber;
    }

    public void setPinNumber(int pinNumber) {
        this.pinNumber = Math.max(0, pinNumber);
    }

    public int getResourceId() {
        return resourceId;
    }

    public CacheableData getPage() {
        return page;
    }


    public void toEvict() {
        this.eviction = true;
    }

    public boolean getEviction() {
        return this.eviction;
    }

    public boolean getPrefetched() {
        return this.prefetched;
    }

    public void setPrefetched(boolean prefetched) {
        this.prefetched = prefetched;
    }
}
