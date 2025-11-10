package com.adaptivescale.rosetta.common.models.dbt;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class AbstractDbtModel {
    private Map<Object, Object> additionalProperties = new HashMap<>();

    @JsonAnyGetter
    public Map<Object, Object> getAdditionalProperties() {
        return additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperties(Map<Object, Object> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    public void addProperty(Object name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public Object getProperty(Object name) {
        if (additionalProperties.containsKey(name))
            return this.additionalProperties.get(name);
        return null;
    }

    public String getPropertyAsString(Object name) {
        return (String) getProperty(name);
    }
}