package com.healthmarketscience.sqlbuilder.custom.sqlserver;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.AlterTableQuery;
import com.healthmarketscience.sqlbuilder.SqlContext;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.dbspec.Table;

import java.io.IOException;

public class MsSqlAlterTableQuery extends AlterTableQuery {
    public MsSqlAlterTableQuery(Table table) {
        super(table);
    }

    public MsSqlAlterTableQuery(Object tableStr) {
        super(tableStr);
    }

    @Override
    protected void appendTo(AppendableExt app, SqlContext newContext) throws IOException {
        final SqlObject action = getAction();
        if (action instanceof MsSqlModifyColumnAction) {
            ((MsSqlModifyColumnAction) action).setColumns(this.getNewColumns());
        }
        super.appendTo(app, newContext);
    }
}
