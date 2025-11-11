package com.adaptivescale.rosetta.common.models.dbt;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collection;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DbtTable extends AbstractDbtModel{
  private String name;
  private String description;
  private Collection<DbtColumn> columns;

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

  public Collection<DbtColumn> getColumns() {
    return columns;
  }

  public void setColumns(Collection<DbtColumn> columns) {
    this.columns = columns;
  }
}