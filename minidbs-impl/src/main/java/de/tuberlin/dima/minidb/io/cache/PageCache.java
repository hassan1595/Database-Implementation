package de.tuberlin.dima.minidb.io.cache;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.*;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.io.cache.CachePinnedException;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.DuplicateCacheEntryException;
import de.tuberlin.dima.minidb.io.cache.EvictedCacheEntry;
import de.tuberlin.dima.minidb.io.cache.PageCache;
import de.tuberlin.dima.minidb.io.cache.PageSize;


public class PageCache {


    private  PageSize pageSize;
    private int numPages;

    private byte[][] binaryPageArray;
    private ArrayList<CacheData> listT1;

    private ArrayList<CacheData> listT2;

    private ArrayList<Pair<Integer, Integer>> listB1;

    private ArrayList<Pair<Integer, Integer>> listB2;

    private float adaptation;

    private int freeIdx;
    public PageCache(PageSize pageSize, int numPages){
        this.pageSize = pageSize;
        this.numPages = numPages;
        this.binaryPageArray = new byte[numPages][pageSize.getNumberOfBytes()];
        this.listT1 = new ArrayList<CacheData>();
        this.listT2 = new ArrayList<CacheData>();
        this.listB1 = new ArrayList<Pair<Integer, Integer>>();
        this.listB2 = new ArrayList<Pair<Integer, Integer>>();
        this.adaptation = 0;
        this.freeIdx = 0;
    }

    private CacheableData getPageAndPinOpt(int resourceId, int pageNumber, boolean pin){

        for (int i = 0; i < this.listT1.size(); i++) {
            CacheData cacheData = this.listT1.get(i);
            if (cacheData.getResourceId() == resourceId
                    && cacheData.getPage().getPageNumber() == pageNumber) {

                if(pin)
                {
                    cacheData.setPinNumber(cacheData.getPinNumber() +1);
                }

                this.listT1.remove(i);
                this.listT1.add(cacheData);

                return cacheData.getPage();
            }
        }

        for (int i = 0; i < this.listT2.size(); i++) {
            CacheData cacheData = this.listT2.get(i);
            if (cacheData.getResourceId() == resourceId
                    && cacheData.getPage().getPageNumber() == pageNumber) {

                if(pin)
                {
                    cacheData.setPinNumber(cacheData.getPinNumber() +1);
                }
                this.listT2.remove(i);
                this.listT2.add(cacheData);

                return cacheData.getPage();
            }
        }

        return null;

    }
    public CacheableData getPage(int resourceId, int pageNumber){

        return this.getPageAndPinOpt(resourceId, pageNumber, false);


    }
    public CacheableData getPageAndPin(int resourceId, int pageNumber){

        return this.getPageAndPinOpt(resourceId, pageNumber, true);

    }

    private int addToBinary(CacheableData newPage){

        if(this.freeIdx == this.numPages)
        {
            return -1;
        }
        int newpagidx = this.freeIdx;
        this.freeIdx++;
        System.arraycopy(this.binaryPageArray[newpagidx], 0, newPage.getBuffer()
                , 0, this.pageSize.getNumberOfBytes());

        return newpagidx;
    }

    private void replacePage(int pagIdx, byte[] buf )
    {
        System.arraycopy(this.binaryPageArray[pagIdx], 0, buf
                , 0, this.pageSize.getNumberOfBytes());
    }
    public EvictedCacheEntry addPage(CacheableData newPage, int resourceId){

        if(this.listT1.isEmpty())
        {
            int newPageIdx = addToBinary(newPage);
            TablePage tp = (TablePage) newPage;
            TablePage tpNew = new TablePage(tp.getSchema(), this.binaryPageArray[newPageIdx]);
            CacheData cacheData   = new CacheData(resourceId, tpNew, newPageIdx);
            this.listT1.add(cacheData);
            return null;
        }

        if(this.listT1.size() > this.adaptation)
        {
            CacheData cd = this.listT1.remove(this.listT1.size() -1);
            TablePage tp = (TablePage) newPage;
            this.replacePage( cd.getPagIdx(), tp.getBuffer());
            cd.getPage().markExpired();
            TablePage tpNew = new TablePage(tp.getSchema(), this.binaryPageArray[cd.getPagIdx()]);
            CacheData newCacheData   = new CacheData(resourceId, tpNew, cd.getPagIdx());
            this.listT1.add(newCacheData);

            Pair<Integer, Integer> b1Pair = new   Pair<>(cd.getResourceId() ,cd.getPage().getPageNumber());
            this.listB1.add(b1Pair);

        }


    }

    public EvictedCacheEntry addPageAndPin(CacheableData newPage, int resourceId) {

    }

    public void unpinPage(int resourceId, int pageNumber) {


    }

    public void unpinAllPages() {


    }

    public int getCapacity(){

        return this.numPages - this.freeIdx;
    }

    CacheableData[] getAllPagesForResource(int resourceId){

    }

    void expelAllPagesForResource(int resourceId){

    }
}
