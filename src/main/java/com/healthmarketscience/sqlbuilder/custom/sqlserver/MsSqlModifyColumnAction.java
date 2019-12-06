package com.healthmarketscience.sqlbuilder.custom.sqlserver;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.Converter;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.SqlObjectList;
import com.healthmarketscience.sqlbuilder.ValidationContext;
import com.healthmarketscience.sqlbuilder.custom.ColumnsModificationAction;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;

import java.io.IOException;
import java.util.Collection;

public class MsSqlModifyColumnAction extends ColumnsModificationAction {
    private SqlObjectList<SqlObject> columns;
    private SqlObject column;

    public MsSqlModifyColumnAction(Object column) {
        this.column = Converter.toCustomTypedColumnSqlObject(column);
    }

    public void setColumns(Collection<DbColumn> columnsForModification) {
        SqlObjectList<SqlObject> sqlObjects = SqlObjectList.create();
        this.columns = sqlObjects.addObjects(Converter.TYPED_COLUMN_TO_OBJ, columnsForModification);
    }

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {
        SqlObject.collectSchemaObjects(column, vContext);
    }

    @Override
    public void appendTo(AppendableExt app) throws IOException {
        if (columns != null && !columns.isEmpty()) {
            String alterTableQueryPart = app.toString();

            app.append(" ")
                    .append("ALTER COLUMN")
                    .append(" ")
                    .append(columns.get(0))
                    .append("\n");

            for (int i = 1; i < columns.size(); i++) {
                app.append(alterTableQueryPart)
                        .append(" ")
                        .append("ALTER COLUMN")
                        .append(" ")
                        .append(columns.get(i))
                        .append("\n");
            }
        } else {
            app.append(" ALTER COLUMN ").append(column);
        }
    }
}
