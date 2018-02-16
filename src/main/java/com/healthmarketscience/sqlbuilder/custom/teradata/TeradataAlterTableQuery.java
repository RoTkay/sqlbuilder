package com.healthmarketscience.sqlbuilder.custom.teradata;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.AlterTableQuery;
import com.healthmarketscience.sqlbuilder.SqlContext;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;

import java.io.IOException;

public class TeradataAlterTableQuery extends AlterTableQuery {
    private DbTable table;

    public TeradataAlterTableQuery(DbTable table) {
        super(table);
        this.table = table;
    }

    @Override
    protected void appendTo(AppendableExt app, SqlContext newContext) throws IOException {
        SqlObject action = this.getAction();
        if (action == null) {
            throw new IllegalArgumentException("Please, provide certain action for \'ALTER TABLE\' command");
        }

        if (action instanceof AddColumnAction || action instanceof TeradataDropColumnAction) {
            app.append("ALTER TABLE ").append(table).append(action);
        }

        if (action instanceof TeradataColumnModificationExpression) {
            TeradataColumnModificationExpression modificationExpression = (TeradataColumnModificationExpression) action;

            modificationExpression.setTable(table);
            modificationExpression.setColumns(getNewColumns());

            app.append(modificationExpression);
        }
    }
}
