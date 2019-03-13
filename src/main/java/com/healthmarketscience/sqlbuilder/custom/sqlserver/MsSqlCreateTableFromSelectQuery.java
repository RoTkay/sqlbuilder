package com.healthmarketscience.sqlbuilder.custom.sqlserver;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.Condition;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.ValidationContext;
import com.healthmarketscience.sqlbuilder.dbspec.Table;

import java.io.IOException;

public class MsSqlCreateTableFromSelectQuery extends SqlObject {

    private Table srcTable;
    private Table destTable;
    private Condition condition;

    public MsSqlCreateTableFromSelectQuery(Table srcTable, Table destTable, Condition condition) {
        this.srcTable = srcTable;
        this.destTable = destTable;
        this.condition = condition;
    }

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {
    }

    @Override
    public void appendTo(AppendableExt app) throws IOException {
        app.append("SELECT * INTO ")
                .append(destTable.getTableNameSQL())
                .append(" FROM ")
                .append(srcTable.getTableNameSQL());

        if (condition != null) {
            app.append(" WHERE ").append(condition);
        }
    }
}
