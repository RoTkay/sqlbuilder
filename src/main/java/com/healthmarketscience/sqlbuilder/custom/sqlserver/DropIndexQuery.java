package com.healthmarketscience.sqlbuilder.custom.sqlserver;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.custom.BaseDropIndexQuery;

import java.io.IOException;

public class DropIndexQuery extends BaseDropIndexQuery {

    private String tableName;
    private String indexName;

    public DropIndexQuery(String tableName, String indexName) {
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public void appendTo(AppendableExt app) throws IOException {
        app.append("DROP INDEX ")
                .append(String.format("%s.%s", tableName, indexName));
    }
}
