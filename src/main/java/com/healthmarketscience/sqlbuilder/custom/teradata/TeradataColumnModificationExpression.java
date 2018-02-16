package com.healthmarketscience.sqlbuilder.custom.teradata;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.*;
import com.healthmarketscience.sqlbuilder.custom.ColumnsModificationAction;
import com.healthmarketscience.sqlbuilder.dbspec.Constraint;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TeradataColumnModificationExpression extends ColumnsModificationAction {
    private static final String TEMP_TABLE_SUFFIX = "_temp";

    private Collection<DbColumn> columns;
    private DbTable table;
    private SqlObject autoIncrementExpression;

    @Override
    public void appendTo(AppendableExt app) throws IOException {
        app.append(createEmptyTableQuery())
                .append("\n")
                .append(copyDataQuery())
                .append("\n")
                .append(dropTempTableQuery())
                .append("\n")
                .append(renameTableQuery());
    }

    private String renameTableQuery() {
        return String.format("RENAME TABLE %s%s TO %s;", table.getName(), TEMP_TABLE_SUFFIX, table.getName());
    }

    private String createEmptyTableQuery() {
        if (columns == null || columns.isEmpty()) {
            throw new RuntimeException("Need to provide table columns for 'ALTER TABLE' command.");
        }

        DbTable tempTable = new DbTable(new DbSchema(new DbSpec(), null), table.getName() + TEMP_TABLE_SUFFIX, null);

        CreateTableQuery createTableQuery = new CreateTableQuery(tempTable);

        createTableQuery.addColumns(columns);

        DbColumn primaryKeyCol = findPrimaryKey(columns);
        createTableQuery.addColumnConstraint(primaryKeyCol, autoIncrementExpression);

        return createTableQuery.toString() + ";";
    }

    private String copyDataQuery() {
        SqlObjectList<SqlObject> columnsList = convertColumnsWithoutAutoIncrementField();
        return String.format("INSERT INTO %s%s(%s) SELECT %s FROM %s;", table.getName(), TEMP_TABLE_SUFFIX, columnsList, columnsList, table.getName());
    }

    private String dropTempTableQuery() {
        return String.format("DROP TABLE %s;", table.getName());
    }

    private DbColumn findPrimaryKey(Collection<DbColumn> columns) {
        return columns.stream()
                .filter(col -> col.getConstraints().stream()
                        .anyMatch(constraint -> constraint.getType() == Constraint.Type.PRIMARY_KEY))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Couldn't find pk."));
    }

    public void setColumns(Collection<DbColumn> columns) {
        this.columns = columns;
    }

    public void setTable(DbTable table) {
        this.table = new DbTable(new DbSchema(new DbSpec(), null), table.getName(), null);
    }

    public void setAutoIncrementExpression(SqlObject autoIncrementExpression) {
        this.autoIncrementExpression = autoIncrementExpression;
    }

    private SqlObjectList<SqlObject> convertColumnsWithoutAutoIncrementField() {
        List<DbColumn> columns = new ArrayList<>(this.columns);
        DbColumn primaryKey = findPrimaryKey(columns);
        columns.remove(primaryKey);

        SqlObjectList<SqlObject> columnsList = SqlObjectList.create();
        return columnsList.addObjects(Converter.CUSTOM_COLUMN_TO_OBJ, columns.toArray());
    }

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {
    }
}
