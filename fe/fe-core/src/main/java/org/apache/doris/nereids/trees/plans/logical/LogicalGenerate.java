// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.DiffOutputInAsterisk;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Generate;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * plan for table generator, the statement like: SELECT * FROM tbl LATERAL VIEW EXPLODE(c1) g as (gc1);
 */
public class LogicalGenerate<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE>
        implements Generate, DiffOutputInAsterisk {

    private final List<Function> generators;
    private final List<Slot> generatorOutput;
    // mapping with function.
    private final List<List<String>> expandColumnAlias;

    public LogicalGenerate(List<Function> generators, List<Slot> generatorOutput, CHILD_TYPE child) {
        this(generators, generatorOutput, ImmutableList.of(), Optional.empty(), Optional.empty(), child);
    }

    public LogicalGenerate(List<Function> generators, List<Slot> generatorOutput, List<List<String>> expandColumnAlias,
            CHILD_TYPE child) {
        this(generators, generatorOutput, expandColumnAlias, Optional.empty(), Optional.empty(), child);
    }

    public LogicalGenerate(List<Function> generators, List<Slot> generatorOutput, List<List<String>> expandColumnAlias,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_GENERATE, groupExpression, logicalProperties, child);
        this.generators = Utils.fastToImmutableList(generators);
        this.generatorOutput = Utils.fastToImmutableList(generatorOutput);
        this.expandColumnAlias = Utils.fastToImmutableList(expandColumnAlias);
    }

    public List<Function> getGenerators() {
        return generators;
    }

    public List<Slot> getGeneratorOutput() {
        return generatorOutput;
    }

    public List<List<String>> getExpandColumnAlias() {
        return expandColumnAlias;
    }

    @Override
    public LogicalGenerate<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalGenerate<>(generators, generatorOutput, expandColumnAlias, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalGenerate(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return generators;
    }

    /**
     * update generators
     */
    public LogicalGenerate<Plan> withGenerators(List<Function> generators) {
        Preconditions.checkArgument(generators.size() == generatorOutput.size());
        List<Slot> newGeneratorOutput = Lists.newArrayList();
        for (int i = 0; i < generators.size(); i++) {
            newGeneratorOutput.add(generatorOutput.get(i).withNullable(generators.get(i).nullable()));
        }
        return new LogicalGenerate<>(generators, newGeneratorOutput, expandColumnAlias, child());
    }

    @Override
    public LogicalGenerate<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalGenerate<>(generators, generatorOutput, expandColumnAlias,
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalGenerate<>(generators, generatorOutput, expandColumnAlias,
                groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.<Slot>builder()
                .addAll(child().getOutput())
                .addAll(generatorOutput)
                .build();
    }

    @Override
    public List<Slot> computeAsteriskOutput() {
        return ImmutableList.<Slot>builder()
                .addAll(child().getAsteriskOutput())
                .addAll(generatorOutput)
                .build();
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("LogicalGenerate",
                "generators", generators,
                "generatorOutput", generatorOutput,
                "stats", statistics
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalGenerate<?> that = (LogicalGenerate<?>) o;
        return generators.equals(that.generators)
                && generatorOutput.equals(that.generatorOutput);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generators, generatorOutput);
    }

    @Override
    public void computeUnique(Builder builder) {
        // don't generate and propagate unique
    }

    @Override
    public void computeUniform(Builder builder) {
        builder.addUniformSlot(child(0).getLogicalProperties().getTrait());
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(child().getLogicalProperties().getTrait());
    }

    @Override
    public void computeFd(Builder builder) {
        builder.addFuncDepsDG(child().getLogicalProperties().getTrait());
    }
}
