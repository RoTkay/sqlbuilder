package com.healthmarketscience.sqlbuilder.custom.teradata;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.*;
import com.healthmarketscience.sqlbuilder.custom.CustomSyntax;
import com.healthmarketscience.sqlbuilder.custom.HookType;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;

import java.io.IOException;

public class TeradataLimitWithOffsetClause extends CustomSyntax implements Verifiable<TeradataLimitWithOffsetClause> {

    private SqlObject rowCount;
    private SqlObject offset;
    private DbColumn tableIdColumn;

    public TeradataLimitWithOffsetClause(Object rowCount, DbColumn dbColumn) {
        this(null, rowCount, dbColumn);
    }

    public TeradataLimitWithOffsetClause(Object offset, Object rowCount, DbColumn dbColumn) {
        this.offset = ((offset != null) ? Converter.toValueSqlObject(offset) : null);
        this.rowCount = Converter.toValueSqlObject(rowCount);
        this.tableIdColumn = dbColumn;
    }

    @Override
    public void apply(SelectQuery query) {
        query.addCustomization(SelectQuery.Hook.FOR_UPDATE, HookType.BEFORE, this);
    }

    @Override
    public void appendTo(AppendableExt appendableExt) throws IOException {
        if (offset == null) {
            offset = new ValueObject(0);
        }

        appendableExt.append(" QUALIFY RANK(")
                .append(tableIdColumn)
                .append(") BETWEEN ")
                .append(offset)
                .append(" AND ")
                .append(rowCount)
                .append(";");
    }

    @Override
    public TeradataLimitWithOffsetClause validate() throws ValidationException {
        return null;
    }

    @Override
    public void validate(ValidationContext vContext) throws ValidationException {
    }

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {
    }
}
