package com.adaptivescale.rosetta.common.models.dbt;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collection;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DbtModel extends AbstractDbtModel{
  private Integer version;
  private Collection<DbtSource> sources;  //sources -> tables
  private Collection<DbtTable> models;    //models (direct list)

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public Collection<DbtSource> getSources() {
    return sources;
  }

  public void setSources(Collection<DbtSource> sources) {
    this.sources = sources;
  }

  public Collection<DbtTable> getModels() {
    return models;
  }

  public void setModels(Collection<DbtTable> models) {
    this.models = models;
  }
}