package sql_evaluator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import sql_evaluator.Table.ColumnDef;
import sql_evaluator.Term.Column;
import sql_evaluator.Term.Literal;

/**
 * Class that evaluates the supplied query and returns the result of projected columns
 */
public class QueryEvaluator {
  private final Query query_;
  private final PrintWriter out_;

  // Mapping of table name, as possibly aliased, to the corresponding table.
  private final Map<String, Table> table_name_map_;
  // Mapping of non-aliased column name to corresponding potentially multiple tables.
  private final Map<String, List<String>> column_table_map_ = new HashMap<>();

  // Map aliased table name to the base index in the cross-product table.
  private final Map<String, Integer> table_name_to_idx_ = new HashMap<>();
  private final ArrayList<ColumnDef> cross_columns_ = new ArrayList<>();
  // Combined cross-product of rows
  private ArrayList<ArrayList<Object>> cross_rows_;
  // Filtered rows
  private final ArrayList<ArrayList<Object>> filtered_rows_ = new ArrayList<>();

  // 'table_name_map' is the map of the table alias name from the "from" clause
  // to the corresponding Table.
  public QueryEvaluator(Query query, PrintWriter out, Map<String, Table> table_name_map) {
    query_ = query;
    out_ = out;

    table_name_map_ = table_name_map;
    // Populate the column name to table name map
    for (Entry<String, Table> name_table : table_name_map_.entrySet()) {
      String table_name = name_table.getKey();
      Table table = name_table.getValue();
      for (ColumnDef column_def : table.columns) {
        column_table_map_.computeIfAbsent(column_def.name, v -> new ArrayList<>())
          .add(table_name);
      }
    }
  }

  // Evaluates the query and writes output to the supplied out writer returning
  // whether any errors were encountered. In case of errors, the error is printed
  // to the supplied output file and simply false is returned by this function.
  public boolean Evaluate() throws IOException {
    if (!ValidateSelectClause()) {
      return false;
    }
    if (!ValidateWhereClause()) {
      return false;
    }
    ComputeCrossProduct();
    FilterRows();
    Table result_table = ProjectRows();
    try (Writer out = new BufferedWriter(out_)) {
      Main.writeTable(out, result_table);
    }
    return true;
  }

  // Given a non-null, non-empty column_name, check whether the column exists in the
  // specified table_name which can be null in which case check across all tables.
  // Returns whether the column is present and in case the column is not present writes
  // the error output to the output file.
  private boolean IsColumnPresent(String column_name, String table_name) {
    if (table_name != null && !table_name.isEmpty()) {
      // Check for column in the specified table
      Table table = table_name_map_.get(table_name);
      if (table == null) {
        out_.println("ERROR: Unknown table name \"" +  table_name  + "\".");
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

  private boolean ValidateSelectClause() {
    for (Selector selector : query_.select) {
      String column_name = selector.source.name;
      assert column_name != null && !column_name.isEmpty();
      String table_name = selector.source.table;

      if (!IsColumnPresent(column_name, table_name)) {
        return false;
      }
    }
    return true;
  }

  // Class used as a return value on processing a where clause Term.
  private static class TermResult {
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

  // Processes the where clause term and returns the result in TermResult.
  private TermResult ProcessTerm(Term term) {
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

  private boolean ValidateWhereClause() {
    for (Condition cond : query_.where) {
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

  private void ComputeCrossProduct() {
    ArrayList<ArrayList<Object>> prev_rows = null;
    // To compute cross product A x B x C..., first multiply A with B and then use the result
    // to multiply with C and so on.
    // "prev_rows" stores the result so far and "curr_table" is the table to be multiplied
    // with prev_rows.
    for (Entry<String, Table> name_table : table_name_map_.entrySet()) {
      String table_name = name_table.getKey();
      Table curr_table = name_table.getValue();
      if (prev_rows == null) {
        prev_rows = new ArrayList<>(curr_table.rows);
        table_name_to_idx_.put(table_name, 0);
        cross_columns_.addAll(curr_table.columns);
        continue;
      }
      ArrayList<ArrayList<Object>> curr_table_rows = curr_table.rows;
      ArrayList<ArrayList<Object>> cross_rows = new ArrayList<>();
      for (ArrayList<Object> prev_row : prev_rows) {
        for (ArrayList<Object> curr_table_row : curr_table_rows) {
          ArrayList<Object> cross_row = new ArrayList<>(prev_row);
          cross_row.addAll(curr_table_row);
          cross_rows.add(cross_row);
        }
      }
      // Discarding the previous intermediate result.
      prev_rows = cross_rows;
      table_name_to_idx_.put(table_name, cross_columns_.size());
      cross_columns_.addAll(curr_table.columns);
    }

    cross_rows_ = prev_rows;
  }

  // Get the value of the term specified in the where clause
  // where the term could be a literal or corresponding to value in a column.
  private Object GetTermValue(Term term, ArrayList<Object> row) {
    TermResult tr = ProcessTerm(term);
    if (tr.is_literal) {
      Literal l = (Literal) term;
      return l.value;
    } else {
      assert tr.is_column_present;
      int i = table_name_to_idx_.get(tr.table_name);
      for (; i < cross_columns_.size(); i++) {
        if (cross_columns_.get(i).name.equals(tr.col_name)) {
          break;
        }
      }
      return row.get(i);
    }
  }

  private void FilterRows() {
    // For every cross row iterate over the where clauses to filter out rows
    for (ArrayList<Object> row : cross_rows_) {
      boolean include_row = true;
      for (Condition cond : query_.where) {
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
        filtered_rows_.add(row);
      }
    }
  }

  // Fetch the projected columns. Return value includes all the selected columns
  // with their alias name and data type.
  // 'col_idxs' is an output parameter returning the indices of selected columns
  // in the cross product table.
  private ArrayList<ColumnDef> GetProjectedColumns(ArrayList<Integer> col_idxs) {
    ArrayList<ColumnDef> proj_cols = new ArrayList<>();

    for (Selector selector : query_.select) {
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
      int i = table_name_to_idx_.get(table_name);
      SqlType type = SqlType.INT;
      for (; i < cross_columns_.size(); i++) {
        if (cross_columns_.get(i).name.equals(column_name)) {
          type = cross_columns_.get(i).type;
          break;
        }
      }
      assert i < cross_columns_.size();
      col_idxs.add(i);
      proj_cols.add(new ColumnDef(alias_name, type));
    }
    return proj_cols;
  }

  private Table ProjectRows() {
    ArrayList<Integer> col_idxs = new ArrayList<>();
    ArrayList<ColumnDef> proj_cols = GetProjectedColumns(col_idxs);
    ArrayList<ArrayList<Object>> proj_rows = new ArrayList<>();

    for (ArrayList<Object> row : filtered_rows_) {
      ArrayList<Object> proj_row = new ArrayList<>();
      for (Integer col_idx : col_idxs) {
        proj_row.add(row.get(col_idx));
      }
      proj_rows.add(proj_row);
    }
    return new Table(proj_cols, proj_rows);
  }
}
