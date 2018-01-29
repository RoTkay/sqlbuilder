package com.healthmarketscience.sqlbuilder.custom;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.Converter;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.ValidationContext;

import java.io.IOException;

public class DropColumnAction extends SqlObject {
    private String columnName;

    public DropColumnAction(String columnName) {
        this.columnName = columnName;
    }

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {

    }

    @Override
    public void appendTo(AppendableExt app) throws IOException {
        app.append(" DROP COLUMN ").append(columnName);
    }
}
