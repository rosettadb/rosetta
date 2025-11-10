package com.adaptivescale.rosetta.common.models.dbt;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DbtColumn extends AbstractDbtModel{
  private String name;
  private String description;

  @JsonProperty("data_tests")
  private Collection<String> tests;

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

  public Collection<String> getTests() {
    return tests;
  }

  public void setTests(Collection<String> tests) {
    this.tests = tests;
  }
}