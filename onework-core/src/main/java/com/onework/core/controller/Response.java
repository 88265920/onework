package com.onework.core.controller;

import lombok.Builder;
import lombok.Singular;

import java.util.List;

@Builder
@SuppressWarnings("rawtypes")
public class Response<T> {
    private int code;
    private String msg;
    @Singular("data")
    private List<T> data;

    public static Response ok() {
        return Response.builder()
                .code(javax.ws.rs.core.Response.Status.OK.getStatusCode())
                .msg("成功")
                .build();
    }

    public static Response error(String msg) {
        return Response.builder()
                .code(javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
                .msg(msg)
                .build();
    }

    public static <T> Response data(List<T> data) {
        return Response.builder()
                .code(javax.ws.rs.core.Response.Status.OK.getStatusCode())
                .msg("成功")
                .data(data)
                .build();
    }
}
