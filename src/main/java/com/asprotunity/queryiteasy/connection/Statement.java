package com.asprotunity.queryiteasy.connection;


import java.util.List;

public interface Statement {
    void execute();
    <ResultType> List<ResultType> executeQuery(RowMapper<ResultType> rowMapper);

    void setString(int position, String value);
    void setInt(int position, int value);

    void setDouble(int position, double value);
}
