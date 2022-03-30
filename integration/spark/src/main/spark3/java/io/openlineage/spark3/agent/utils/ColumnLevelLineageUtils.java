package io.openlineage.spark3.agent.utils;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.types.StructType;

/**
 * Utility functions for detecting column level lineage within {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan}.
 */
@Slf4j
public class ColumnLevelLineageUtils {

  // FIXME: implement test that checks the behaviour on different SQL queries
  public static ColumnLevelLineageBuilder includeColumnLevelLineage(
      StructType schema, LogicalPlan plan) {

    ColumnLevelLineageBuilder columnLevelLineageBuilder = new ColumnLevelLineageBuilder(schema);

    discoverInputs(plan, columnLevelLineageBuilder);
    discoverOutputs(plan, columnLevelLineageBuilder);

    log.info(columnLevelLineageBuilder.toString());
    log.info(plan.prettyJson());

    return columnLevelLineageBuilder;
  }

  private static void discoverOutputs(
      LogicalPlan plan, ColumnLevelLineageBuilder columnLevelLineageBuilder) {
    if (columnLevelLineageBuilder.hasOutputs()) {
      return;
    }

    ScalaConversionUtils.fromSeq(plan.output()).stream()
        .filter(attr -> attr instanceof Attribute)
        .map(attr -> (Attribute) attr)
        .forEach(attr -> columnLevelLineageBuilder.addOutput(attr.exprId(), attr.name()));

    // check for Aggregate
    if (plan instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) plan;
      ScalaConversionUtils.<NamedExpression>fromSeq(aggregate.aggregateExpressions()).stream()
          .forEach(expr -> columnLevelLineageBuilder.addOutput(expr.exprId(), expr.name()));
    }

    if (!columnLevelLineageBuilder.hasOutputs()) {
      // try to extract it deeper
      plan.children()
          .foreach(
              child -> {
                discoverOutputs(child, columnLevelLineageBuilder);
                return scala.runtime.BoxedUnit.UNIT;
              });
    }
    return;
  }

  private static void discoverInputs(
      LogicalPlan plan, ColumnLevelLineageBuilder columnLevelLineageBuilder) {
    plan.children()
        .foreach(
            node -> {
              node.foreach(
                  innerNode -> {
                    discoverInputsFromNode(innerNode, columnLevelLineageBuilder);
                    return scala.runtime.BoxedUnit.UNIT;
                  });
              return scala.runtime.BoxedUnit.UNIT;
            });
    return;
  }

  private static void discoverInputsFromNode(
      LogicalPlan node, ColumnLevelLineageBuilder columnLevelLineageBuilder) {
    if (node instanceof Union) {
      handleUnion((Union) node, columnLevelLineageBuilder);
    }

    Optional<DataSourceV2Relation> relation = extractV2Relation(node);
    if (relation.isPresent()) {
      ScalaConversionUtils.fromSeq(node.output()).stream()
          .filter(attr -> attr instanceof AttributeReference)
          .map(attr -> (AttributeReference) attr)
          .forEach(
              attr ->
                  columnLevelLineageBuilder.addInput(attr.exprId(), relation.get(), attr.name()));
    }
  }

  private static Optional<DataSourceV2Relation> extractV2Relation(LogicalPlan node) {
    if (node instanceof DataSourceV2Relation) {
      return Optional.of((DataSourceV2Relation) node);
    } else if (node instanceof DataSourceV2ScanRelation) {
      return Optional.of(((DataSourceV2ScanRelation) node).relation());
    }
    return Optional.empty();
  }

  private static void handleUnion(
      Union union, ColumnLevelLineageBuilder columnLevelLineageBuilder) {
    // implement in Java code equivalent to Scala 'children.map(_.output).transpose.map { attrs =>'
    List<LogicalPlan> children = ScalaConversionUtils.<LogicalPlan>fromSeq(union.children());
    List<ArrayList<Attribute>> childrenAttributes = new LinkedList<>();
    children.forEach(
        c ->
            childrenAttributes.add(
                new ArrayList<>(ScalaConversionUtils.<Attribute>fromSeq(c.output()))));

    // max attributes size
    int maxAttributeSize =
        childrenAttributes.stream().map(l -> l.size()).max(Integer::compare).get();

    IntStream.range(0, maxAttributeSize)
        .forEach(
            position -> {
              ExprId firstExpr = childrenAttributes.get(0).get(position).exprId();
              IntStream.range(1, children.size())
                  .forEach(
                      childIndex -> {
                        ArrayList<Attribute> attributes = childrenAttributes.get(childIndex);
                        columnLevelLineageBuilder.exprAConsumesResultOfExprB(
                            firstExpr, attributes.get(position).exprId());
                      });
            });
  }
}
