package com.csg.airtel.aaa4j.domain.model;

import com.csg.airtel.aaa4j.domain.util.ResponseCodeEnum;

import java.util.List;

public class BaseResponse<T> {

    private String status;
    private String message;
    private List<T> data; // generic list of any type
    private PageDetails pageDetails;

    // Constructors
    public BaseResponse(String status, String message, List<T> data, PageDetails pageDetails) {
        this.status = status;
        this.message = message;
        this.data = data;
        this.pageDetails = pageDetails;
    }

    // SUCCESS factory
    public static <T> BaseResponse<T> success(String message, List<T> data, PageDetails pageDetails) {
        return new BaseResponse<>(ResponseCodeEnum.SUCCESSFUL.code(), message, data, pageDetails);
    }

    // ERROR factory
    public static <T> BaseResponse<T> error(String status, String message) {
        return new BaseResponse<>(status, message, null, null);
    }


    // Getters and Setters
    public String getStatus() {return status;}

    public void setStatus(String status) {this.status = status;}

    public String getMessage() {return message;}

    public void setMessage(String message) {this.message = message;}

    public List<T> getData() {return data;}

    public void setData(List<T> data) {this.data = data;}

    public PageDetails getPageDetails() {return pageDetails;}

    public void setPageDetails(PageDetails pageDetails) {this.pageDetails = pageDetails;}
}
