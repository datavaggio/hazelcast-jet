/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.parse.SqlCreateJob;
import com.hazelcast.jet.sql.impl.parse.SqlShowStatement;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorTable;

import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;
import static com.hazelcast.jet.sql.impl.validate.ValidatorResource.RESOURCE;
import static org.apache.calcite.sql.SqlKind.AGGREGATE;

public class JetSqlValidator extends HazelcastSqlValidator {

    private boolean isCreateJob;

    public JetSqlValidator(
            SqlValidatorCatalogReader catalogReader,
            HazelcastTypeFactory typeFactory,
            SqlConformance conformance
    ) {
        super(JetSqlOperatorTable.instance(), catalogReader, typeFactory, conformance);
    }

    @Override
    public SqlNode validate(SqlNode topNode) {
        if (topNode instanceof SqlCreateJob) {
            isCreateJob = true;
        }

        if (topNode.getKind().belongsTo(SqlKind.DDL)) {
            topNode.validate(this, getEmptyScope());
            return topNode;
        }

        if (topNode instanceof SqlShowStatement) {
            return topNode;
        }

        return super.validate(topNode);
    }

    @Override
    public void validateInsert(SqlInsert insert) {
        super.validateInsert(insert);

        if (!isCreateJob && containsStreamingSource(insert.getSource())) {
            throw newValidationError(insert, RESOURCE.mustUseCreateJob());
        }
    }

    @Override
    protected void validateGroupClause(SqlSelect select) {
        super.validateGroupClause(select);

        if (containsGroupingOrAggregation(select) && containsStreamingSource(select)) {
            throw newValidationError(select, RESOURCE.streamingAggregationsNotSupported());
        }
    }

    private boolean containsGroupingOrAggregation(SqlSelect select) {
        if (select.getGroup() != null && select.getGroup().size() > 0) {
            return true;
        }

        if (select.isDistinct()) {
            return true;
        }

        for (SqlNode node : select.getSelectList()) {
            if (node.getKind().belongsTo(AGGREGATE)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Goes over all the referenced tables in the given {@link SqlNode}
     * and returns true if any of them uses a streaming connector.
     */
    private boolean containsStreamingSource(SqlNode node) {
        class FindStreamingTablesVisitor extends SqlBasicVisitor<Void> {
            boolean found;

            @Override
            public Void visit(SqlIdentifier id) {
                SqlValidatorTable table = getCatalogReader().getTable(id.names);
                if (table != null) { // not every identifier is a table
                    HazelcastTable hazelcastTable = table.unwrap(HazelcastTable.class);
                    SqlConnector connector = getJetSqlConnector(hazelcastTable.getTarget());
                    if (connector.isStream()) {
                        found = true;
                    }
                }
                return super.visit(id);
            }
        }

        FindStreamingTablesVisitor visitor = new FindStreamingTablesVisitor();
        node.accept(visitor);
        return visitor.found;
    }
}
