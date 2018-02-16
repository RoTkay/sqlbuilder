package com.healthmarketscience.sqlbuilder.custom.teradata;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.ValidationContext;

import java.io.IOException;

public class TeradataDropColumnAction extends SqlObject {
    private String columnName;

    public TeradataDropColumnAction(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public void appendTo(AppendableExt app) throws IOException {
        app.append(" DROP ").append(columnName);
    }

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {
    }
}
