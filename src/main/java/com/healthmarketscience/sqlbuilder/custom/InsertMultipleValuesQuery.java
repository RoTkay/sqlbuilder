package com.healthmarketscience.sqlbuilder.custom;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.common.util.AppendeeObject;
import com.healthmarketscience.sqlbuilder.*;
import com.healthmarketscience.sqlbuilder.dbspec.Table;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.UnhandledException;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class InsertMultipleValuesQuery extends InsertQuery {
    private SqlObjectList<SqlObjectList<SqlObject>> queryValues;
    private final Converter<Object, SqlObject> valueToObjectConverter;
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private boolean needEscapeQuotes;

    public InsertMultipleValuesQuery(Table table) {
        super(table);
        valueToObjectConverter = new Converter<Object, SqlObject>() {
            @Override
            public SqlObject convert(Object src) {
                if (src instanceof Date) {
                    return new ValueObject(dateFormat.format(src));
                }

                if (src instanceof String) {
                    return !((String) src).isEmpty() ? new ValueObject(validateInput(String.valueOf(src))) : SqlObject.NULL_VALUE;
                }

                return toValueSqlObject(src);
            }
        };
    }

    private String validateInput(String input) {
        String val = StringEscapeUtils.escapeSql(input);

        return needEscapeQuotes ? escapeJavaStyleString(val, false, false) : val;
    }

    public InsertMultipleValuesQuery addColumns(Collection<DbColumn> columns, List<List<Object>> values) {
        queryValues = SqlObjectList.create();
        _columns = SqlObjectList.create();

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

    public SqlObjectList<SqlObjectList<SqlObject>> getQueryValues() {
        return queryValues;
    }

    public Converter<Object, SqlObject> getValueToObjectConverter() {
        return valueToObjectConverter;
    }

    public void setNeedEscapeQuotes(boolean needEscapeQuotes) {
        this.needEscapeQuotes = needEscapeQuotes;
    }

    private static String escapeJavaStyleString(String str, boolean escapeSingleQuotes, boolean escapeForwardSlash) {
        if (str == null) {
            return null;
        }
        try {
            StringWriter writer = new StringWriter(str.length() * 2);
            escapeJavaStyleString(writer, str, escapeSingleQuotes, escapeForwardSlash);
            return writer.toString();
        } catch (IOException ioe) {
            // this should never ever happen while writing to a StringWriter
            throw new UnhandledException(ioe);
        }
    }

    private static void escapeJavaStyleString(Writer out, String str, boolean escapeSingleQuote,
                                              boolean escapeForwardSlash) throws IOException {
        if (out == null) {
            throw new IllegalArgumentException("The Writer must not be null");
        }
        if (str == null) {
            return;
        }
        int sz;
        sz = str.length();
        for (int i = 0; i < sz; i++) {
            char ch = str.charAt(i);

            if (ch < 32) {
                switch (ch) {
                    case '\b':
                        out.write('\\');
                        out.write('b');
                        break;
                    case '\n':
                        out.write('\\');
                        out.write('n');
                        break;
                    case '\t':
                        out.write('\\');
                        out.write('t');
                        break;
                    case '\f':
                        out.write('\\');
                        out.write('f');
                        break;
                    case '\r':
                        out.write('\\');
                        out.write('r');
                        break;
                    default:
                        out.write(ch);
                        break;
                }
            } else {
                switch (ch) {
                    case '\'':
                        if (escapeSingleQuote) {
                            out.write('\\');
                        }
                        out.write('\'');
                        break;
                    case '"':
                        out.write('\\');
                        out.write('"');
                        break;
                    case '\\':
                        out.write('\\');
                        out.write('\\');
                        break;
                    case '/':
                        if (escapeForwardSlash) {
                            out.write('\\');
                        }
                        out.write('/');
                        break;
                    default:
                        out.write(ch);
                        break;
                }
            }
        }
    }
}
