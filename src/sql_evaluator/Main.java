package sql_evaluator;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.io.File;
import java.io.PrintWriter;
import java.io.Writer;
import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javafx.util.Pair;
import sql_evaluator.Table.ColumnDef;
import sql_evaluator.Term.Column;
import sql_evaluator.Term.Literal;


public final class Main {
    private static Map<String, Table> table_name_map_ = new HashMap<>();
    private static Map<String, List<String>> column_table_map_ = new HashMap<>();

    // Given a non-null, non-empty column_name, check whether the column exists in the
    // specified table_name which can be null in which case check across all tables.
    public static boolean IsColumnPresent(String column_name, String table_name,
                                          PrintWriter err) {
        if (table_name != null && !table_name.isEmpty()) {
            // Check for column in the specified table
            Table table = table_name_map_.get(table_name);
            if (table == null) {
                err.println("ERROR: Unknown table name \"" +  table_name  + "\".");
                return false;
            }
            if (table.columns.stream().noneMatch(col -> col.name.equals(column_name))) {
                err.println("ERROR: Column reference \"" + column_name +
                  "\" not found in table \"" + table_name + "\"");
                return false;
            }
        } else {
            // No table specified. So lookup the tables that match in our column to table map.
            List<String> matching_tables = column_table_map_.get(column_name);
            int num_matches = matching_tables != null ? matching_tables.size() : 0;
            if (num_matches != 1) {
                assert matching_tables != null;
                if (num_matches == 0) {
                    err.println("ERROR: Column reference \"" + column_name +
                      "\" not found in any table");
                } else  {
                    assert num_matches > 1;
                    List<String> quoted_matching_tables = matching_tables.stream()
                      .map(t -> "\"" + t + "\"").collect(Collectors.toList());
                    err.println("ERROR: Column reference \"" + column_name +
                      "\" is ambiguous; present in multiple tables: " +
                      String.join(", ", quoted_matching_tables) + ".");
                }
                return false;
            }
        }
        return true;
    }

    public static  boolean ValidateSelectClause(Query query, PrintWriter err) {
        for (Selector selector : query.select) {
            String column_name = selector.source.name;
            assert column_name != null && !column_name.isEmpty();
            String table_name = selector.source.table;

            if (!IsColumnPresent(column_name, table_name, err)) {
                return false;
            }
        }
        return true;
    }


    public static Pair<Boolean, SqlType> IsColumnPresentAndGetSqlType(Term term,
                                                                      PrintWriter err) {
        SqlType type;
        if (term instanceof Column) {
            Column col = (Column) term;
            String col_name = col.ref.name;
            String table_name = col.ref.table;
            if (!IsColumnPresent(col_name, table_name, err)) {
                return new Pair<>(false, SqlType.INT);
            }
            if (table_name == null) {
                List<String> table_names = column_table_map_.get(col_name);
                assert table_names != null && table_names.size() == 1;
                table_name = table_names.get(0);
            }
            assert table_name != null && !table_name.isEmpty();
            Table table = table_name_map_.get(table_name);
            ColumnDef col_def = table.columns.stream()
              .filter(c -> c.name.equals(col_name)).findFirst().orElse(null);
            assert col_def != null;
            type = col_def.type;
        } else {
            assert term instanceof Literal;
            Literal lit = (Literal) term;
            if (lit.value instanceof String) {
                type = SqlType.STR;
            } else {
                type = SqlType.INT;
            }
        }
        return new Pair<>(true, type);
    }

    public static boolean ValidateWhereClause(Query query,
                                              PrintWriter err) {
        for (Condition cond : query.where) {
            // If the left and right terms are not literals then check whether the specified
            // columns are present in the tables.
            Pair<Boolean, SqlType> p_left = IsColumnPresentAndGetSqlType(cond.left, err);
            if (!p_left.getKey()) {
                return false;
            }
            SqlType left_type = p_left.getValue();

            Pair<Boolean, SqlType> p_right = IsColumnPresentAndGetSqlType(cond.right, err);
            if (!p_right.getKey()) {
                return false;
            }
            SqlType right_type = p_right.getValue();

            if (left_type != right_type) {
                err.println("ERROR: Incompatible types to \"" + cond.op.symbol + "\": " +
                  left_type.name + " and " + right_type.name + ".");
                return false;
            }
        }
        return true;
    }


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

        Set<Table> tables = new HashSet<>();
        // Starter code effectively validating the from clause. Only minor updates.
        for (TableDecl tableDecl : query.from) {
            String tableSourcePath = tableFolder + File.separator + (tableDecl.source + ".table.json");
            Table table;
            try {
                table = JacksonUtil.readFromFile(tableSourcePath, Table.class);
            } catch (JsonProcessingException ex) {
                System.err.println("Error loading \"" + tableSourcePath + "\" as table JSON: " + ex.getMessage());
                System.exit(1); return;
            }
            tables.add(table);
            assert table_name_map_.get(tableDecl.name) != null;
            table_name_map_.put(tableDecl.name, table);
        }

        // Populate the column name to table name map
        for (Entry<String, Table> name_table : table_name_map_.entrySet()) {
            String table_name = name_table.getKey();
            Table table = name_table.getValue();
            for (ColumnDef column_def : table.columns) {
                column_table_map_.computeIfAbsent(column_def.name, v -> new ArrayList<>())
                  .add(table_name);
            }
        }

        int exit_code = 0;
        do {
            try (PrintWriter printWriter = new PrintWriter(outputFile)) {
                if (!ValidateSelectClause(query, printWriter)) {
                    exit_code = 1;
                    break;
                }
                if (!ValidateWhereClause(query, printWriter)) {
                    exit_code = 1;
                    break;
                }

                // TODO: Actually evaluate query.
                // For now, just dump the input back out.
                try (Writer out = new BufferedWriter(printWriter)) {
                    JacksonUtil.writeIndented(out, query);

                    for (Table table : tables) {
                        writeTable(out, table);
                    }
                }
            }
        } while (false);

        if (exit_code != 0) {
            System.exit(exit_code);
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
