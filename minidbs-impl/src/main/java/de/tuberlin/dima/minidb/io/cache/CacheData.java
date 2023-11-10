package de.tuberlin.dima.minidb.io.cache;

import de.tuberlin.dima.minidb.io.tables.TablePage;

public class CacheData{

    private CacheableData page;
    private int resourceId;
    private int pinNumber;

    private int pagIdx;

    public CacheData(int resourceId, CacheableData page, int pagIdx){
        this.resourceId = resourceId;
        this.page = page;
        this.pinNumber = 0;
        this.pagIdx = pagIdx;
    }


    public int getPinNumber() {
        return pinNumber;
    }

    public void setPinNumber(int pinNumber) {
        this.pinNumber = pinNumber;
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
}
