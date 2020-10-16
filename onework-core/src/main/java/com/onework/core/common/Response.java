package com.onework.core.common;

import lombok.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

@Data
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

    @SneakyThrows
    public static Response error(Exception e) {
        @Cleanup StringWriter sw = new StringWriter();
        @Cleanup PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return Response.builder()
                .code(javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
                .msg(sw.toString())
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
