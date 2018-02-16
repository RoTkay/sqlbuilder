package com.healthmarketscience.sqlbuilder.custom;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.Converter;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.SqlObjectList;
import com.healthmarketscience.sqlbuilder.ValidationContext;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;

import java.io.IOException;
import java.util.List;

public class ColumnsModificationAction extends SqlObject {
    private SqlObjectList<SqlObject> columnsForModification;

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {
    }

    public SqlObjectList<SqlObject> getColumnsForModification() {
        return columnsForModification;
    }

    public void setColumnsForModification(List<DbColumn> columnsForModification) {
        SqlObjectList<SqlObject> sqlObjects = SqlObjectList.create();

        this.columnsForModification = sqlObjects.addObjects(Converter.TYPED_COLUMN_TO_OBJ, columnsForModification);
    }

    protected void appendTo(AppendableExt app, String modificationCommand) throws IOException {
        String alterTableQueryPart = app.toString();

        app.append(" ")
                .append(modificationCommand)
                .append(" ")
                .append(columnsForModification.get(0))
                .append("\n");

        for (int i = 1; i < columnsForModification.size(); i++) {
            app.append(alterTableQueryPart)
                    .append(" ")
                    .append(modificationCommand)
                    .append(" ")
                    .append(columnsForModification.get(i))
                    .append("\n");
        }
    }
}