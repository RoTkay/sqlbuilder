package com.healthmarketscience.sqlbuilder.custom;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.common.util.AppendeeObject;
import com.healthmarketscience.sqlbuilder.*;
import com.healthmarketscience.sqlbuilder.dbspec.Table;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class InsertBatchMultipleValuesQuery extends InsertMultipleValuesQuery {
    public InsertBatchMultipleValuesQuery(Table table) {
        super(table);
    }

    @Override
    protected void appendTo(AppendableExt app, SqlContext newContext) throws IOException {
        newContext.setUseTableAliases(false);

        SqlObjectList<SqlObjectList<SqlObject>> values = getQueryValues();

        values.forEach(row -> appendBatchToInsertQuery(row, app));
    }

    private void appendBatchToInsertQuery(SqlObjectList<SqlObject> row, AppendableExt app) {
        appendPrefixTo(app);

        String singleBatch = StreamSupport.stream(row.spliterator(), false)
                .map(AppendeeObject::toString)
                .collect(Collectors.joining(",", "(", ")"));

        try {
            app.append("VALUES").append(singleBatch).append(";");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void validate(ValidationContext vContext) throws ValidationException {
    }

    @Override
    protected void appendPrefixTo(AppendableExt app){
        try {
            super.appendPrefixTo(app);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}