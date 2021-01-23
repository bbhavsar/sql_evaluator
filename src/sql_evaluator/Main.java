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
import sql_evaluator.Condition.Op;
import sql_evaluator.Table.ColumnDef;
import sql_evaluator.Term.Column;
import sql_evaluator.Term.Literal;


public final class Main {
    private static Map<String, Table> table_name_map_ = new HashMap<>();
    private static Map<String, List<String>> column_table_map_ = new HashMap<>();
    private static PrintWriter out_;

    private static class CrossTable {
        // Map aliased table name to the base index in the cross-product table.
        private Map<String, Integer> table_name_to_idx = new HashMap<>();
        private ArrayList<ColumnDef> cross_columns = new ArrayList<>();
        //
        // Combined cross-product of rows
        private ArrayList<ArrayList<Object>> cross_rows = new ArrayList<>();

        public void ComputeCrossProduct() {
            ArrayList<ArrayList<Object>> prev_rows = null;

            for (Entry<String, Table> name_table : table_name_map_.entrySet()) {
                String table_name = name_table.getKey();
                Table curr_table = name_table.getValue();
                if (prev_rows == null) {
                    prev_rows = new ArrayList<>(curr_table.rows);
                    table_name_to_idx.put(table_name, 0);
                    cross_columns.addAll(curr_table.columns);
                    continue;
                }
                ArrayList<ArrayList<Object>> curr_table_rows = curr_table.rows;
                ArrayList<ArrayList<Object>> cross_rows = new ArrayList<>();
                for (int i = 0; i < prev_rows.size(); i++) {
                    for (int j = 0; j < curr_table_rows.size(); j++) {
                        ArrayList<Object> cross_row = new ArrayList<>(prev_rows.get(i));
                        cross_row.addAll(curr_table_rows.get(j));
                        cross_rows.add(cross_row);
                    }
                }
                prev_rows = cross_rows;
                table_name_to_idx.put(table_name, cross_columns.size());
                cross_columns.addAll(curr_table.columns);
            }

            cross_rows = prev_rows;
        }

        // Filtered rows
        private ArrayList<ArrayList<Object>> filtered_rows = new ArrayList<>();

        private Object GetTermValue(Term term, ArrayList<Object> row) {
            TermResult tr = ProcessTerm(term);
            if (tr.is_literal) {
                Literal l = (Literal) term;
                return l.value;
            } else {
                assert tr.is_column_present;
                int i = table_name_to_idx.get(tr.table_name);
                for (; i < cross_columns.size(); i++) {
                    if (cross_columns.get(i).name.equals(tr.col_name)) {
                        break;
                    }
                }
                return row.get(i);
            }
        }

        public ArrayList<ArrayList<Object>> FilterRows(Query query) {
            // For every cross row iterate over the where clauses to filter out rows
            ArrayList<ArrayList<Object>> filtered_rows = new ArrayList<>();
            for (ArrayList<Object> row : cross_rows) {
                boolean include_row = true;
                for (Condition cond : query.where) {
                    Object l_val = GetTermValue(cond.left, row);
                    Object r_val = GetTermValue(cond.right, row);
                    int compare_result;
                    if (l_val instanceof String) {
                        assert r_val instanceof String;
                        compare_result = ((String) l_val).compareTo((String) r_val);
                    } else {
                        assert l_val instanceof Integer;
                        assert r_val instanceof Integer;
                        compare_result = ((Integer) l_val).compareTo((Integer) r_val);
                    }
                    switch (cond.op) {
                        case EQ:
                            if (compare_result != 0) include_row = false;
                            break;
                        case NE:
                            if (compare_result == 0) include_row = false;
                            break;
                        case GT:
                            if (compare_result <= 0) include_row = false;
                            break;
                        case GE:
                            if (compare_result < 0) include_row = false;
                        case LT:
                            if (compare_result >= 0) include_row = false;
                            break;
                        case LE:
                            if (compare_result > 0) include_row = false;
                            break;
                        default:
                            assert false;
                    }
                    if (!include_row) break;
                }
                if (include_row) {
                    filtered_rows.add(row);
                }
            }
            return filtered_rows;
        }

        ArrayList<ColumnDef> GetProjectedColumns(Query query, ArrayList<Integer> col_idxs) {
            ArrayList<ColumnDef> proj_cols = new ArrayList<>();

            for (Selector selector : query.select) {
                String alias_name = selector.name;
                String column_name = selector.source.name;
                assert column_name != null && !column_name.isEmpty();
                String table_name = selector.source.table;
                if (table_name == null) {
                    List<String> matching_tables = column_table_map_.get(column_name);
                    assert matching_tables.size() == 1;
                    table_name = matching_tables.get(0);
                }
                assert table_name != null;
                int i = table_name_to_idx.get(table_name);
                SqlType type = SqlType.INT;
                for (; i < cross_columns.size(); i++) {
                    if (cross_columns.get(i).name.equals(column_name)) {
                        type = cross_columns.get(i).type;
                        break;
                    }
                }
                assert i < cross_columns.size();
                col_idxs.add(i);
                proj_cols.add(new ColumnDef(alias_name, type));
            }

            return proj_cols;
        }

        public Table ProjectRows(Query query,
                                 ArrayList<ArrayList<Object>> filtered_rows) {
            ArrayList<Integer> col_idxs = new ArrayList<>();
            ArrayList<ColumnDef> proj_cols = GetProjectedColumns(query, col_idxs);
            ArrayList<ArrayList<Object>> proj_rows = new ArrayList<>();

            for (ArrayList<Object> row : filtered_rows) {
                ArrayList<Object> proj_row = new ArrayList<>();
                for (Integer col_idx : col_idxs) {
                    proj_row.add(row.get(col_idx));
                }
                proj_rows.add(proj_row);
            }
            return new Table(proj_cols, proj_rows);
        }
    };

    // Given a non-null, non-empty column_name, check whether the column exists in the
    // specified table_name which can be null in which case check across all tables.
    public static boolean IsColumnPresent(String column_name, String table_name) {
        if (table_name != null && !table_name.isEmpty()) {
            // Check for column in the specified table
            Table table = table_name_map_.get(table_name);
            if (table == null) {
                out_.println("out_OR: Unknown table name \"" +  table_name  + "\".");
                return false;
            }
            if (table.columns.stream().noneMatch(col -> col.name.equals(column_name))) {
                out_.println("ERROR: Column reference \"" + column_name +
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
                    out_.println("ERROR: Column reference \"" + column_name +
                      "\" not found in any table");
                } else  {
                    assert num_matches > 1;
                    List<String> quoted_matching_tables = matching_tables.stream()
                      .map(t -> "\"" + t + "\"").collect(Collectors.toList());
                    out_.println("ERROR: Column reference \"" + column_name +
                      "\" is ambiguous; present in multiple tables: " +
                      String.join(", ", quoted_matching_tables) + ".");
                }
                return false;
            }
        }
        return true;
    }

    public static  boolean ValidateSelectClause(Query query) {
        for (Selector selector : query.select) {
            String column_name = selector.source.name;
            assert column_name != null && !column_name.isEmpty();
            String table_name = selector.source.table;

            if (!IsColumnPresent(column_name, table_name)) {
                return false;
            }
        }
        return true;
    }

    public static class TermResult {
        public boolean is_column_present;
        public SqlType sql_type;
        public boolean is_literal;
        public String table_name;
        public String col_name;
        TermResult(boolean col_present, SqlType type, boolean literal, String tb_name,
                   String c_name) {
            is_column_present = col_present;
            sql_type = type;
            is_literal = literal;
            table_name = tb_name;
            col_name = c_name;
        }
    }

    public static TermResult ProcessTerm(Term term) {
        SqlType type;
        boolean is_literal;
        String table_name;
        String col_name;
        if (term instanceof Column) {
            is_literal = false;
            Column col = (Column) term;
            col_name = col.ref.name;
            table_name = col.ref.table;
            if (!IsColumnPresent(col_name, table_name)) {
                return new TermResult(false, SqlType.INT, false, table_name, col_name);
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
            is_literal = true;
            Literal lit = (Literal) term;
            if (lit.value instanceof String) {
                type = SqlType.STR;
            } else {
                type = SqlType.INT;
            }
            table_name = col_name = null;
        }
        return new TermResult(true, type, is_literal, table_name, col_name);
    }

    public static boolean ValidateWhereClause(Query query) {
        for (Condition cond : query.where) {
            // If the left and right terms are not literals then check whether the specified
            // columns are present in the tables.
            TermResult p_left = ProcessTerm(cond.left);
            if (!p_left.is_column_present) {
                return false;
            }
            SqlType left_type = p_left.sql_type;

            TermResult p_right = ProcessTerm(cond.right);
            if (!p_right.is_column_present) {
                return false;
            }
            SqlType right_type = p_right.sql_type;

            if (left_type != right_type) {
                out_.println("ERROR: Incompatible types to \"" + cond.op.symbol + "\": " +
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
            try (PrintWriter out_ = new PrintWriter(outputFile)) {
                if (!ValidateSelectClause(query)) {
                    exit_code = 1;
                    break;
                }
                if (!ValidateWhereClause(query)) {
                    exit_code = 1;
                    break;
                }

                CrossTable ct = new CrossTable();
                ct.ComputeCrossProduct();
                ArrayList<ArrayList<Object>> filtered_rows = ct.FilterRows(query);
                Table result_table = ct.ProjectRows(query, filtered_rows);
                try (Writer out = new BufferedWriter(out_)) {
                    writeTable(out, result_table);
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
