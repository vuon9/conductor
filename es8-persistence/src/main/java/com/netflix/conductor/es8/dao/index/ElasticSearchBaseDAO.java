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
package com.netflix.conductor.es8.dao.index;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.es8.dao.query.parser.Expression;
import com.netflix.conductor.es8.dao.query.parser.internal.ParserException;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.fasterxml.jackson.databind.ObjectMapper;

abstract class ElasticSearchBaseDAO implements IndexDAO {

    String indexPrefix;
    ObjectMapper objectMapper;

    String loadTypeMappingSource(String path) throws IOException {
        return IOUtils.toString(
                ElasticSearchBaseDAO.class.getResourceAsStream(path), Charset.defaultCharset());
    }

    Query boolQueryBuilder(String expression, String queryString) throws ParserException {
        Query query;
        if (StringUtils.isNotEmpty(expression)) {
            Expression exp = Expression.fromString(expression);
            query = exp.getFilterBuilder();
        } else {
            query = new Query.Builder().matchAll(m -> m).build();
        }

        Query.Builder builder = new Query.Builder();
        builder.bool(b -> b.must(m -> m.queryString(q -> q.query(queryString))).must(query));

        return builder.build();
    }

    protected String getIndexName(String documentType) {
        return indexPrefix + "_" + documentType;
    }
}
