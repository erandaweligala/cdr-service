package com.csg.airtel.aaa4j.domain.model;

public class PageDetails {
    private long totalRecords;
    private int pageNumber;
    private int pageElementCount;

    public PageDetails(){
    }

    public PageDetails(long totalRecords, int pageNumber, int pageElementCount){
        this.totalRecords = totalRecords;
        this.pageNumber = pageNumber;
        this.pageElementCount = pageElementCount;
    }

    public int getPageElementCount() {
        return pageElementCount;
    }

    public void setPageElementCount(int pageElementCount) {
        this.pageElementCount = pageElementCount;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(long totalRecords) {
        this.totalRecords = totalRecords;
    }
}
