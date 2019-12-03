package com.healthmarketscience.sqlbuilder.custom.sqlserver;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.Converter;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.ValueObject;
import com.healthmarketscience.sqlbuilder.custom.InsertMultipleValuesQuery;
import com.healthmarketscience.sqlbuilder.dbspec.Table;

import java.io.IOException;
import java.util.Date;

public class MsSqlInsertMultipleValuesQuery extends InsertMultipleValuesQuery {
    public MsSqlInsertMultipleValuesQuery(Table table, boolean unicodeString) {
        super(table);
        if (unicodeString) {
            valueToObjectConverter = new Converter<Object, SqlObject>() {
                @Override
                public SqlObject convert(Object src) {
                    if (src instanceof Date) {
                        return new ValueObject(dateFormat.format(src));
                    }

                    if (src instanceof String) {
                        return !((String) src).isEmpty() ? new UnicodeValueObject(validateInput(String.valueOf(src))) : SqlObject.NULL_VALUE;
                    }

                    return toValueSqlObject(src);
                }
            };
        }
    }

    private static class UnicodeValueObject extends ValueObject {
        public UnicodeValueObject(Object value) {
            super(value);
        }

        @Override
        public void appendTo(AppendableExt app) throws IOException {
            app.append("N");
            super.appendTo(app);
        }
    }
}
