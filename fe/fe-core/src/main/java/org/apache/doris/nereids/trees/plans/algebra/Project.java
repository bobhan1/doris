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

package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.plans.logical.ProjectMergeable;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Common interface for logical/physical project.
 */
public interface Project extends ProjectMergeable {
    List<NamedExpression> getProjects();

    /**
     * Generate a map that the key is the alias slot, corresponding value is the expression produces the slot.
     * For example:
     * <pre>
     * projects:
     * [a, alias(b as c), alias((d + e + 1) as f)]
     * result map:
     * c -> b
     * f -> d + e + 1
     * </pre>
     */
    default Map<Slot, Expression> getAliasToProducer() {
        return ExpressionUtils.generateReplaceMap(getProjects());
    }

    /**
     * combine upper level and bottom level projections
     * 1. alias combination, for example
     * proj(x as y, b) --> proj(a as x, b, c) =>(a as y, b)
     * 2. remove used projection in bottom project
     * @param childProject bottom project
     * @return project list for merged project
     */
    default List<NamedExpression> mergeProjections(Project childProject) {
        List<NamedExpression> projects = new ArrayList<>(
                PlanUtils.mergeProjections(childProject.getProjects(), getProjects())
        );
        for (NamedExpression expression : childProject.getProjects()) {
            // keep NoneMovableFunction for later use
            if (expression.containsType(NoneMovableFunction.class)) {
                projects.add(expression);
            }
        }
        return projects;
    }

    /** check can merge two projects */
    default boolean canMergeChildProjections(Project childProject) {
        return childProject.canMergeParentProjections(getProjects());
    }

    /** check can merge two projects */
    default boolean canMergeParentProjections(List<NamedExpression> parentProject) {
        List<NamedExpression> bottomProjects = getProjects();
        if (ExpressionUtils.containsWindowExpression(parentProject)
                && ExpressionUtils.containsWindowExpression(bottomProjects)) {
            return false;
        }

        return PlanUtils.canReplaceWithProjections(bottomProjects, parentProject);
    }

    /**
     * find projects, if not found the slot, then throw AnalysisException
     */
    static List<? extends Expression> findProject(
            Collection<? extends Expression> expressions,
            List<? extends NamedExpression> projects) throws AnalysisException {
        Map<ExprId, NamedExpression> exprIdToProject = projects.stream()
                .collect(ImmutableMap.toImmutableMap(NamedExpression::getExprId, p -> p));

        return ExpressionUtils.rewriteDownShortCircuit(expressions,
                expr -> {
                    if (expr instanceof Slot) {
                        Slot slot = (Slot) expr;
                        ExprId exprId = slot.getExprId();
                        NamedExpression project = exprIdToProject.get(exprId);
                        if (project == null) {
                            throw new AnalysisException("ExprId " + slot.getExprId() + " no exists in " + projects);
                        }
                        return project;
                    }
                    return expr;
                });
    }

    /** isAllSlots */
    default boolean isAllSlots() {
        for (NamedExpression project : getProjects()) {
            if (!project.isSlot()) {
                return false;
            }
        }
        return true;
    }

    /**
     * project(A as B) is eventually slot project, where A is a slot
     */
    default boolean isEventuallyAllSlots() {
        for (NamedExpression project : getProjects()) {
            if (!project.isSlot() && !(project instanceof Alias && project.child(0) instanceof Slot)) {
                return false;
            }
        }
        return true;
    }

    /** containsNoneMovableFunction */
    default boolean containsNoneMovableFunction() {
        for (NamedExpression expression : getProjects()) {
            if (expression.containsType(NoneMovableFunction.class)) {
                return true;
            }
        }
        return false;
    }
}
