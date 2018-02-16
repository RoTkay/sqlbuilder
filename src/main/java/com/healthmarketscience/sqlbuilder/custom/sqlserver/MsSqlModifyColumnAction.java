package com.healthmarketscience.sqlbuilder.custom.sqlserver;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.Converter;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.SqlObjectList;
import com.healthmarketscience.sqlbuilder.ValidationContext;
import com.healthmarketscience.sqlbuilder.custom.ColumnsModificationAction;

import java.io.IOException;

public class MsSqlModifyColumnAction extends ColumnsModificationAction {

    private SqlObject column;

    public MsSqlModifyColumnAction(Object column) {
        this.column = Converter.toCustomTypedColumnSqlObject(column);
    }

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {
        SqlObject.collectSchemaObjects(column, vContext);
    }

    @Override
    public void appendTo(AppendableExt app) throws IOException {
        SqlObjectList<SqlObject> columns = getColumnsForModification();

        if (columns != null && !columns.isEmpty()) {
            super.appendTo(app, "ALTER COLUMN");
        } else {
            app.append(" ALTER COLUMN ").append(column);
        }
    }
}
