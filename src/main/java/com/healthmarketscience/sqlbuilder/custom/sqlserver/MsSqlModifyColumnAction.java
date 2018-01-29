package com.healthmarketscience.sqlbuilder.custom.sqlserver;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.Converter;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.ValidationContext;

import java.io.IOException;

public class MsSqlModifyColumnAction extends SqlObject {

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
        app.append(" ALTER COLUMN ").append(column);
    }
}
