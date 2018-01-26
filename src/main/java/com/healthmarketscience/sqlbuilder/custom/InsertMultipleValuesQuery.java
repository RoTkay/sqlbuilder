package com.healthmarketscience.sqlbuilder.custom;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.common.util.AppendeeObject;
import com.healthmarketscience.sqlbuilder.*;
import com.healthmarketscience.sqlbuilder.dbspec.Table;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class InsertMultipleValuesQuery extends InsertQuery {
    private SqlObjectList<SqlObjectList<SqlObject>> queryValues = SqlObjectList.create();
    private final Converter<Object, SqlObject> valueToObjectConverter;
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public InsertMultipleValuesQuery(Table table) {
        super(table);
        valueToObjectConverter = new Converter<Object, SqlObject>() {
            @Override
            public SqlObject convert(Object src) {
                if (src instanceof Date) {
                    return new ValueObject(dateFormat.format(src));
                }

                return toValueSqlObject(src);
            }
        };
    }

    public InsertMultipleValuesQuery addColumns(Collection<DbColumn> columns, List<List<Object>> values) {
        _columns.addObjects(Converter.CUSTOM_COLUMN_TO_OBJ, columns.toArray());

        for (List<Object> row : values) {
            SqlObjectList<SqlObject> rowOfValues = SqlObjectList.create();
            rowOfValues.addObjects(valueToObjectConverter, row.toArray());
            queryValues.addObject(rowOfValues);
        }

        return this;
    }

    @Override
    protected void appendTo(AppendableExt app, SqlContext newContext) throws IOException {
        newContext.setUseTableAliases(false);

        appendPrefixTo(app);

        String values = StreamSupport.stream(queryValues.spliterator(), false)
                .map(row -> StreamSupport.stream(row.spliterator(), false)
                        .map(AppendeeObject::toString)
                        .collect(Collectors.joining(",", "(", ")")))
                .collect(Collectors.joining(","));

        app.append("VALUES ").append(values);
    }

    @Override
    public void validate(ValidationContext vContext) throws ValidationException {
    }
}
