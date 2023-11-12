package de.tuberlin.dima.minidb.io.cache;

import de.tuberlin.dima.minidb.io.tables.TablePage;

public class CacheData{

    private CacheableData page;
    private int resourceId;
    private int pinNumber;

    private int pagIdx;

    private boolean eviction;

    private boolean prefetched;

    public CacheData(int resourceId, CacheableData page, int pagIdx){
        this.resourceId = resourceId;
        this.page = page;
        this.pinNumber = 0;
        this.pagIdx = pagIdx;
        this.eviction = false;
        this.prefetched = true;
    }


    public int getPinNumber() {
        return pinNumber;
    }

    public void setPinNumber(int pinNumber) {
        this.pinNumber = Math.max(0,pinNumber);
    }

    public int getResourceId() {
        return resourceId;
    }

    public CacheableData getPage() {
        return page;
    }

    public int getPagIdx() {
        return pagIdx;
    }

    public void toEvict(){
        this.eviction = true;
    }

    public boolean getEviction(){
        return this.eviction;
    }

    public boolean getPrefetched()
    {
        return this.prefetched;
    }

    public void setPrefetched(boolean prefetched){
        this.prefetched = prefetched;
    }
}
