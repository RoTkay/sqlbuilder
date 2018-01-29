package com.healthmarketscience.sqlbuilder.custom.mysql;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.Converter;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.ValidationContext;

import java.io.IOException;

public class MySqlModifyColumnAction extends SqlObject {

    private SqlObject column;

    public MySqlModifyColumnAction(Object column) {
        this.column = Converter.toCustomTypedColumnSqlObject(column);
    }

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {
        SqlObject.collectSchemaObjects(column, vContext);
    }

    @Override
    public void appendTo(AppendableExt app) throws IOException {
        app.append(" MODIFY COLUMN ").append(column);
    }
}
