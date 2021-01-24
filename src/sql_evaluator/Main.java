package sql_evaluator;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.io.File;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Main {
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: COMMAND <table-folder> <sql-json-file> <output-file>");
            System.exit(1); return;
        }

        String tableFolder = args[0];
        String sqlJsonFile = args[1];
        String outputFile = args[2];

        Query query;
        try {
            query = JacksonUtil.readFromFile(sqlJsonFile, Query.class);
        } catch (JsonProcessingException ex) {
            System.err.println("Error loading \"" + sqlJsonFile + "\" as query JSON: " + ex.getMessage());
            System.exit(1); return;
        }

        // Starter code effectively validating the from clause. Only minor updates.
        // Mapping of table name, possibly aliased, to the corresponding table.
        Map<String, Table> table_name_map = new HashMap<>();
        for (TableDecl tableDecl : query.from) {
            String tableSourcePath = tableFolder + File.separator + (tableDecl.source + ".table.json");
            Table table;
            try {
                table = JacksonUtil.readFromFile(tableSourcePath, Table.class);
            } catch (JsonProcessingException ex) {
                System.err.println("Error loading \"" + tableSourcePath + "\" as table JSON: " + ex.getMessage());
                System.exit(1); return;
            }
            assert table_name_map.get(tableDecl.name) == null;
            table_name_map.put(tableDecl.name, table);
        }

        try (PrintWriter out = new PrintWriter(outputFile)) {
            QueryEvaluator qe = new QueryEvaluator(query, out, table_name_map);
            qe.Evaluate();
        }
    }

    public static void writeTable(Writer out, Table table) throws IOException {
        out.write("[\n");

        out.write("    ");
        JacksonUtil.write(out, table.columns);

        for (List<Object> row : table.rows) {
            out.write(",\n    ");
            JacksonUtil.write(out, row);
        }

        out.write("\n]\n");
    }
}
