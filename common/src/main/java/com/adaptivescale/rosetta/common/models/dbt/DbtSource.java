package com.adaptivescale.rosetta.common.models.dbt;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collection;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DbtSource extends AbstractDbtModel{
  private String name;
  private String description;
  private Collection<DbtTable> tables;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Collection<DbtTable> getTables() {
    return tables;
  }

  public void setTables(Collection<DbtTable> tables) {
    this.tables = tables;
  }
}