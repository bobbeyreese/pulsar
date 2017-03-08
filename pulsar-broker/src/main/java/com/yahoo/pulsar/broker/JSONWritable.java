package com.yahoo.pulsar.broker;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;

public class JSONWritable {

    @JsonIgnore
    public byte[] getJsonBytes() throws JsonProcessingException {
        return ObjectMapperFactory.getThreadLocal().writeValueAsBytes(this);
    }

    @JsonIgnore
    public String getJsonString() throws JsonProcessingException {
        return ObjectMapperFactory.getThreadLocal().writeValueAsString(this);
    }
}
