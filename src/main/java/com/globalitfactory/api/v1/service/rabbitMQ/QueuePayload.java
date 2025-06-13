package com.globalitfactory.api.v1.service.rabbitMQ;

import lombok.Builder;

import java.io.Serializable;

@Builder
public class QueuePayload implements Serializable {
    private String objectType;
    private String syncType;
    private Object payload;
}
