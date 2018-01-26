package com.healthmarketscience.sqlbuilder.custom;

import com.healthmarketscience.common.util.AppendableExt;
import com.healthmarketscience.sqlbuilder.SqlObject;
import com.healthmarketscience.sqlbuilder.ValidationContext;

import java.io.IOException;
import java.util.function.Consumer;

public class CustomSqlObject extends SqlObject {
    private Consumer<AppendableExt> consumer;

    public CustomSqlObject(Consumer<AppendableExt> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void appendTo(AppendableExt appendableExt) throws IOException {
        consumer.accept(appendableExt);
    }

    @Override
    protected void collectSchemaObjects(ValidationContext vContext) {
    }
}
