/*
 * Copyright 2023 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.es8.dao.query.parser;

import java.io.InputStream;
import java.util.stream.Collectors;

import com.netflix.conductor.es8.dao.query.parser.internal.*;
import com.netflix.conductor.es8.dao.query.parser.internal.ComparisonOp.Operators;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;

/**
 * @author Viren
 *     <pre>
 * Represents an expression of the form as below:
 * key OPR value
 * OPR is the comparison operator which could be on the following:
 * 	&gt;, &lt;, = , !=, IN, BETWEEN
 *         </pre>
 */
public class NameValue extends AbstractNode implements FilterProvider {

    private Name name;

    private ComparisonOp op;

    private ConstValue value;

    private Range range;

    private ListConst valueList;

    public NameValue(InputStream is) throws ParserException {
        super(is);
    }

    @Override
    protected void _parse() throws Exception {
        this.name = new Name(is);
        this.op = new ComparisonOp(is);

        if (this.op.getOperator().equals(Operators.BETWEEN.value())) {
            this.range = new Range(is);
        } else if (this.op.getOperator().equals(Operators.IN.value())) {
            this.valueList = new ListConst(is);
        } else {
            this.value = new ConstValue(is);
        }
    }

    @Override
    public String toString() {
        return "" + name + op + value;
    }

    /**
     * @return the name
     */
    public Name getName() {
        return name;
    }

    /**
     * @return the op
     */
    public ComparisonOp getOp() {
        return op;
    }

    /**
     * @return the value
     */
    public ConstValue getValue() {
        return value;
    }

    @Override
    public Query getFilterBuilder() {
        if (op.getOperator().equals(Operators.EQUALS.value())) {
            return new Query.Builder()
                    .queryString(q -> q.query(name.getName() + ":" + value.getValue().toString()))
                    .build();
        } else if (op.getOperator().equals(Operators.BETWEEN.value())) {
            return new Query.Builder()
                    .range(
                            r ->
                                    r.field(name.getName())
                                            .gte(JsonData.of(range.getLow()))
                                            .lte(JsonData.of(range.getHigh())))
                    .build();
        } else if (op.getOperator().equals(Operators.IN.value())) {
            return new Query.Builder()
                    .terms(
                            t ->
                                    t.field(name.getName())
                                            .terms(
                                                    tb ->
                                                            tb.value(
                                                                    valueList.getList().stream()
                                                                            .map(
                                                                                    v ->
                                                                                            new FieldValue
                                                                                                            .Builder()
                                                                                                    .stringValue(
                                                                                                            (String)
                                                                                                                    v)
                                                                                                    .build())
                                                                            .collect(
                                                                                    Collectors
                                                                                            .toList()))))
                    .build();
        } else if (op.getOperator().equals(Operators.NOT_EQUALS.value())) {
            return new Query.Builder()
                    .queryString(
                            q ->
                                    q.query(
                                            "NOT "
                                                    + name.getName()
                                                    + ":"
                                                    + value.getValue().toString()))
                    .build();
        } else if (op.getOperator().equals(Operators.GREATER_THAN.value())) {
            return new Query.Builder()
                    .range(r -> r.field(name.getName()).gt(JsonData.of(value.getValue())))
                    .build();
        } else if (op.getOperator().equals(Operators.IS.value())) {
            if (value.getSysConstant().equals(ConstValue.SystemConsts.NULL)) {
                return new Query.Builder()
                        .bool(b -> b.mustNot(mn -> mn.exists(e -> e.field(name.getName()))))
                        .build();
            } else if (value.getSysConstant().equals(ConstValue.SystemConsts.NOT_NULL)) {
                return new Query.Builder().exists(e -> e.field(name.getName())).build();
            }
        } else if (op.getOperator().equals(Operators.LESS_THAN.value())) {
            return new Query.Builder()
                    .range(r -> r.field(name.getName()).lt(JsonData.of(value.getValue())))
                    .build();
        } else if (op.getOperator().equals(Operators.STARTS_WITH.value())) {
            return new Query.Builder()
                    .prefix(p -> p.field(name.getName()).value(value.getUnquotedValue()))
                    .build();
        }

        throw new IllegalStateException("Incorrect/unsupported operators");
    }
}
