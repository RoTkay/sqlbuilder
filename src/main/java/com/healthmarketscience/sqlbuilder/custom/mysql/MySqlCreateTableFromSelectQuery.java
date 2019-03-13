package com.healthmarketscience.sqlbuilder.custom.mysql;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.Condition;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.ValidationContext;
import com.healthmarketscience.sqlbuilder.dbspec.Table;

import java.io.IOException;

public class MySqlCreateTableFromSelectQuery extends SqlObject {

    private Table srcTable;
    private Table destTable;
    private Condition condition;

    public MySqlCreateTableFromSelectQuery(Table srcTable, Table destTable, Condition condition) {
        this.srcTable = srcTable;
        this.destTable = destTable;
        this.condition = condition;
    }

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {
    }

    @Override
    public void appendTo(AppendableExt app) throws IOException {
        app.append("CREATE TABLE ")
                .append(destTable.getTableNameSQL())
                .append(" SELECT * FROM ")
                .append(srcTable.getTableNameSQL());

        if (condition != null) {
            app.append(" WHERE ").append(condition);
        }
    }
}
