package com.healthmarketscience.sqlbuilder.custom.sqlserver;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.*;
import com.healthmarketscience.sqlbuilder.custom.CustomSyntax;
import com.healthmarketscience.sqlbuilder.custom.HookType;

import java.io.IOException;

public class MsSqlLimitWithOffsetClause extends CustomSyntax implements Verifiable<MsSqlLimitWithOffsetClause> {
    private SqlObject rowCount;
    private SqlObject offset;

    public MsSqlLimitWithOffsetClause(Object rowCount) {
        this(null, rowCount);
    }

    public MsSqlLimitWithOffsetClause(Object offset, Object rowCount) {
        this.offset = ((offset != null) ? Converter.toValueSqlObject(offset) : null);
        this.rowCount = Converter.toValueSqlObject(rowCount);
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

        appendableExt.append(" OFFSET ")
                .append(offset)
                .append(" ROWS ")
                .append("FETCH NEXT ")
                .append(rowCount)
                .append(" ROWS ONLY");
    }

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {

    }

    @Override
    public MsSqlLimitWithOffsetClause validate() throws ValidationException {
        return null;
    }

    @Override
    public void validate(ValidationContext vContext) throws ValidationException {

    }
}