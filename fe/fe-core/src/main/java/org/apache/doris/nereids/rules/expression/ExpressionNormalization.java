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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.rules.expression.rules.ConcatWsMultiArrayToOne;
import org.apache.doris.nereids.rules.expression.rules.ConvertAggStateCast;
import org.apache.doris.nereids.rules.expression.rules.DigitalMaskingConvert;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.expression.rules.InPredicateDedup;
import org.apache.doris.nereids.rules.expression.rules.InPredicateExtractNonConstant;
import org.apache.doris.nereids.rules.expression.rules.InPredicateToEqualToRule;
import org.apache.doris.nereids.rules.expression.rules.JsonFunctionRewrite;
import org.apache.doris.nereids.rules.expression.rules.LogToLn;
import org.apache.doris.nereids.rules.expression.rules.MedianConvert;
import org.apache.doris.nereids.rules.expression.rules.MergeDateTrunc;
import org.apache.doris.nereids.rules.expression.rules.NormalizeBinaryPredicatesRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyArithmeticComparisonRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyArithmeticRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyCastRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyNotExprRule;
import org.apache.doris.nereids.rules.expression.rules.SupportJavaDateFormatter;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * normalize expression of plan rule set.
 */
public class ExpressionNormalization extends ExpressionRewrite {
    // we should run supportJavaDateFormatter before foldConstantRule or be will fold
    // from_unixtime(timestamp, 'yyyyMMdd') to 'yyyyMMdd'
    // specically note: LogToLn and ConcatWsMultiArrayToOne must  before FoldConstantRule,otherwise log will core when
    // input single argument like log(100),and concat_ws will retuen a wrong result when input multi array
    public static final List<ExpressionRewriteRule<ExpressionRewriteContext>> NORMALIZE_REWRITE_RULES
                = ImmutableList.of(
            bottomUp(
                SupportJavaDateFormatter.INSTANCE,
                NormalizeBinaryPredicatesRule.INSTANCE,
                InPredicateDedup.INSTANCE,
                InPredicateExtractNonConstant.INSTANCE,
                InPredicateToEqualToRule.INSTANCE,
                SimplifyNotExprRule.INSTANCE,
                SimplifyArithmeticRule.INSTANCE,
                LogToLn.INSTANCE,
                ConcatWsMultiArrayToOne.INSTANCE,
                FoldConstantRule.INSTANCE,
                SimplifyCastRule.INSTANCE,
                DigitalMaskingConvert.INSTANCE,
                MedianConvert.INSTANCE,
                SimplifyArithmeticComparisonRule.INSTANCE,
                ConvertAggStateCast.INSTANCE,
                MergeDateTrunc.INSTANCE,
                JsonFunctionRewrite.INSTANCE,
                CheckCast.INSTANCE
            )
    );

    public ExpressionNormalization() {
        super(new ExpressionRuleExecutor(NORMALIZE_REWRITE_RULES));
    }

    @Override
    public Expression rewrite(Expression expression, ExpressionRewriteContext expressionRewriteContext) {
        return super.rewrite(expression, expressionRewriteContext);
    }
}
