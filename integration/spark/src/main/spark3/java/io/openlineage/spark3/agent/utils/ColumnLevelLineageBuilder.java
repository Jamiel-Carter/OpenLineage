package io.openlineage.spark3.agent.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class ColumnLevelLineageBuilder {

  private Map<ExprId, ExprId> exprDependencies = new HashMap<>();
  private Map<ExprId, List<Pair<DataSourceV2Relation, String>>> inputs = new HashMap<>();
  private Map<ExprId, StructField> outputs = new HashMap<>();
  private final StructType schema;

  public ColumnLevelLineageBuilder(StructType schema) {
    this.schema = schema;
  }

  public void addInput(ExprId exprId, DataSourceV2Relation relation, String attributeName) {
    if (!inputs.containsKey(exprId)) {
      inputs.put(exprId, new LinkedList<>());
    }
    inputs.get(exprId).add(Pair.of(relation, attributeName));
  }

  public void addOutput(ExprId exprId, String attributeName) {
    Arrays.stream(schema.fields())
        .filter(sf -> sf.name().equals(attributeName))
        .findAny()
        .ifPresent(field -> outputs.put(exprId, field));
  }

  public void exprAConsumesResultOfExprB(ExprId exprIdA, ExprId exprIdB) {
    // FIXME: improve to handle one to many dependencies
    exprDependencies.put(exprIdA, exprIdB);
  }

  public void build() {
    // TODO: build column lineage level facet
  }

  public boolean hasOutputs() {
    return !outputs.isEmpty();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ColumnLevelLineage" + System.lineSeparator());
    outputs
        .keySet()
        .forEach(
            exprId -> {
              StructField outputField = outputs.get(exprId);

              // find all expressions needed to evaluate the output
              List<ExprId> exprIds = findDependentInputs(exprId);

              // get all dependent inputs
              List<Pair<DataSourceV2Relation, String>> inputFields =
                  exprIds.stream()
                      .filter(inputExprId -> inputs.containsKey(inputExprId))
                      .flatMap(inputExprId -> inputs.get(inputExprId).stream())
                      .filter(Objects::nonNull)
                      .collect(Collectors.toList());

              sb.append("output ").append(outputField.name()).append(" depends on [");
              inputFields.stream()
                  .forEach(
                      pair -> {
                        sb.append(pair.getLeft().name())
                            .append(".")
                            .append(pair.getRight())
                            .append(", ");
                      });
              sb.append("]").append(System.lineSeparator());
            });
    return sb.toString();
  }

  // TODO: temp method for PoC testing
  public List<Pair<DataSourceV2Relation, String>> getInputsUsedFor(String outputName) {
    ExprId outputExprId =
        outputs.keySet().stream()
            .filter(exprId -> outputs.get(exprId).name().equalsIgnoreCase(outputName))
            .findAny()
            .get();

    return findDependentInputs(outputExprId).stream()
        .filter(inputExprId -> inputs.containsKey(inputExprId))
        .flatMap(inputExprId -> inputs.get(inputExprId).stream())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private List<ExprId> findDependentInputs(ExprId outputExprId) {
    List<ExprId> dependentInputs = new LinkedList<>();
    dependentInputs.add(outputExprId);
    boolean continueSearch = true;

    while (continueSearch) {
      Set<ExprId> newDependentInputs =
          dependentInputs.stream()
              .filter(exprId -> exprDependencies.containsKey(exprId))
              .map(exprId -> exprDependencies.get(exprId))
              .filter(exprId -> !dependentInputs.contains(exprId)) // filter already added
              .collect(Collectors.toSet());

      dependentInputs.addAll(newDependentInputs);
      continueSearch = !newDependentInputs.isEmpty();
    }

    return dependentInputs;
  }
}
