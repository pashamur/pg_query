class PgQuery
  class AExprParser
    attr_reader :val, :var

    def initialize(aexpr)
      @aexpr = aexpr

      if @aexpr["kind"] == AEXPR_IN # IN condition
        @var = @aexpr["lexpr"][COLUMN_REF]["fields"].map{|f| fetch_val(f)}.join(".")
        @val = "(" + @aexpr["rexpr"].map{|f| fetch_val(f[A_CONST]["val"])}.join(",") + ")"
      elsif @aexpr["lexpr"].is_a?(Hash) && @aexpr["rexpr"].is_a?(Hash)
        hsh = @aexpr["lexpr"].merge(@aexpr["rexpr"])

        if hsh[A_CONST] && hsh[COLUMN_REF]
          @val = fetch_val(hsh[A_CONST]["val"])
          @var = hsh[COLUMN_REF]["fields"].map{|f| fetch_val(f)}.join(".")
        else
          @val = @var = nil
        end
      elsif get_name.downcase == "between"
        @var = @aexpr["lexpr"][COLUMN_REF]["fields"].map{|f| fetch_val(f)}.join(".")
        @val = @aexpr["rexpr"].map{|hsh| fetch_val(hsh[A_CONST]["val"]) }
      else
        @var = @val = nil
      end
    end

    def fetch_val(segment)
      if segment.keys[0].downcase == "string"
        segment.values[0]["str"]
      elsif segment.keys[0].downcase == "integer"
        segment.values[0]["ival"]
      elsif segment.keys[0].downcase == "float"
        segment.values[0]["str"].to_f
      end
    end

    def get_conditions
      return nil if @var.nil? || @val.nil?
      if get_name.downcase == "between"
        [[get_var, ">=", get_val[0]], [get_var, "<=", get_val[1]]]
      else
        [get_var, get_name, get_val]
      end
    end

    def get_name
      return "IN" if @aexpr["kind"] == AEXPR_IN
      fetch_val(@aexpr["name"].first)
    end

    def get_val
      @val
    end

    def get_var
      @var
    end
  end

  # Returns a list of columns that the query filters by - this excludes the
  # target list, but includes things like JOIN condition and WHERE clause.
  #
  # Note: This also traverses into sub-selects.
  def filter_columns # rubocop:disable Metrics/CyclomaticComplexity
    load_tables_and_aliases! if @aliases.nil?

    # Get condition items from the parsetree
    statements = @tree.dup
    condition_items = []
    filter_columns = []
    loop do
      statement = statements.shift
      if statement
        if statement[SELECT_STMT]
          if statement[SELECT_STMT]['op'] == 0
            if statement[SELECT_STMT][FROM_CLAUSE_FIELD]
              # FROM subselects
              statement[SELECT_STMT][FROM_CLAUSE_FIELD].each do |item|
                next unless item['RangeSubselect']
                statements << item['RangeSubselect']['subquery']
              end

              # JOIN ON conditions
              condition_items += conditions_from_join_clauses(statement[SELECT_STMT][FROM_CLAUSE_FIELD])
            end

            # WHERE clause
            condition_items << statement[SELECT_STMT]['whereClause'] if statement[SELECT_STMT]['whereClause']

            # CTEs
            if statement[SELECT_STMT]['withClause']
              statement[SELECT_STMT]['withClause']['WithClause']['ctes'].each do |item|
                statements << item['CommonTableExpr']['ctequery'] if item['CommonTableExpr']
              end
            end
          elsif statement[SELECT_STMT]['op'] == 1
            statements << statement[SELECT_STMT]['larg'] if statement[SELECT_STMT]['larg']
            statements << statement[SELECT_STMT]['rarg'] if statement[SELECT_STMT]['rarg']
          end
        elsif statement['UpdateStmt']
          condition_items << statement['UpdateStmt']['whereClause'] if statement['UpdateStmt']['whereClause']
        elsif statement['DeleteStmt']
          condition_items << statement['DeleteStmt']['whereClause'] if statement['DeleteStmt']['whereClause']
        end
      end

      # Process both JOIN and WHERE condition_items here
      next_item = condition_items.shift
      if next_item
        if next_item[A_EXPR]
          %w(lexpr rexpr).each do |side|
            expr = next_item.values[0][side]
            next unless expr && expr.is_a?(Hash)
            condition_items << expr
          end
        elsif next_item[BOOL_EXPR]
          condition_items += next_item[BOOL_EXPR]['args']
        elsif next_item[ROW_EXPR]
          condition_items += next_item[ROW_EXPR]['args']
        elsif next_item[COLUMN_REF]
          column, table = next_item[COLUMN_REF]['fields'].map { |f| f['String']['str'] }.reverse
          filter_columns << [@aliases[table] || table, column]
        elsif next_item[NULL_TEST]
          condition_items << next_item[NULL_TEST]['arg']
        elsif next_item[FUNC_CALL]
          # FIXME: This should actually be extracted as a funccall and be compared with those indices
          condition_items += next_item[FUNC_CALL]['args'] if next_item[FUNC_CALL]['args']
        elsif next_item[SUB_LINK]
          condition_items << next_item[SUB_LINK]['testexpr']
          statements << next_item[SUB_LINK]['subselect']
        end
      end

      break if statements.empty? && condition_items.empty?
    end

    filter_columns.uniq
  end

  def simple_where_conditions # rubocop:disable Metrics/CyclomaticComplexity
    load_tables_and_aliases! if @aliases.nil?

    # Get condition items from the parsetree
    statements = @tree.dup
    condition_items = []
    filter_columns = []
    conditions = []

    loop do
      statement = statements.shift
      if statement
        if statement[SELECT_STMT]
          if statement[SELECT_STMT]['op'] == 0
            if statement[SELECT_STMT][FROM_CLAUSE_FIELD]
              # FROM subselects
              statement[SELECT_STMT][FROM_CLAUSE_FIELD].each do |item|
                next unless item['RangeSubselect']
                statements << item['RangeSubselect']['subquery']
              end

              # JOIN ON conditions
              condition_items += conditions_from_join_clauses(statement[SELECT_STMT][FROM_CLAUSE_FIELD])
            end

            # WHERE clause
            condition_items << statement[SELECT_STMT]['whereClause'] if statement[SELECT_STMT]['whereClause']

            # CTEs
            if statement[SELECT_STMT]['withClause']
              statement[SELECT_STMT]['withClause']['WithClause']['ctes'].each do |item|
                statements << item['CommonTableExpr']['ctequery'] if item['CommonTableExpr']
              end
            end
          elsif statement[SELECT_STMT]['op'] == 1
            statements << statement[SELECT_STMT]['larg'] if statement[SELECT_STMT]['larg']
            statements << statement[SELECT_STMT]['rarg'] if statement[SELECT_STMT]['rarg']
          end
        elsif statement['UpdateStmt']
          condition_items << statement['UpdateStmt']['whereClause'] if statement['UpdateStmt']['whereClause']
        elsif statement['DeleteStmt']
          condition_items << statement['DeleteStmt']['whereClause'] if statement['DeleteStmt']['whereClause']
        end
      end

      bool_test = {
        0 => ["IS", true],
        1 => ["IS NOT", true],
        2 => ["IS", false],
        3 => ["IS NOT", false]
      }

      null_test = {
        0 => ["IS", "NULL"],
        1 => ["IS NOT", "NULL"]
      }

      # Process both JOIN and WHERE conditions here
      next_item = condition_items.shift
      if next_item
        if next_item[A_EXPR]
          cond = AExprParser.new(next_item[A_EXPR]).get_conditions
          if cond && cond[0].is_a?(Array)
            conditions += cond
          else
            conditions << cond
          end

          %w(lexpr rexpr).each do |side|
            expr = next_item.values[0][side]
            next unless expr && expr.is_a?(Hash)
            condition_items << expr
          end
        elsif next_item[BOOL_EXPR]
          condition_items += next_item[BOOL_EXPR]['args']
        elsif next_item[ROW_EXPR]
          condition_items += next_item[ROW_EXPR]['args']
        elsif next_item[COLUMN_REF]
          column, table = next_item[COLUMN_REF]['fields'].map { |f| f['String']['str'] }.reverse
          filter_columns << [@aliases[table] || table, column]
        elsif next_item[NULL_TEST]
          condition_items << next_item[NULL_TEST]['arg']

          # Add nulltest to conditions (var IS NULL...)
          if next_item[NULL_TEST]['arg'][COLUMN_REF]
            var = next_item[NULL_TEST]['arg'][COLUMN_REF]['fields'].map { |f| f['String']['str'] }.join(".")
            conditions << [var, null_test[next_item[NULL_TEST]['nulltesttype'].to_i]].flatten
          end
        elsif next_item[BOOLEAN_TEST]
          condition_items << next_item[BOOLEAN_TEST]['arg']

          # Add nulltest to conditions (var IS NULL...)
          if next_item[BOOLEAN_TEST]['arg'][COLUMN_REF]
            var = next_item[BOOLEAN_TEST]['arg'][COLUMN_REF]['fields'].map { |f| f['String']['str'] }.join(".")
            conditions << [var, bool_test[next_item[BOOLEAN_TEST]['booltesttype'].to_i]].flatten
          end
        elsif next_item[FUNC_CALL]
          # FIXME: This should actually be extracted as a funccall and be compared with those indices
          condition_items += next_item[FUNC_CALL]['args'] if next_item[FUNC_CALL]['args']
        elsif next_item[SUB_LINK]
          condition_items << next_item[SUB_LINK]['testexpr']
          statements << next_item[SUB_LINK]['subselect']
        end
      end

      break if statements.empty? && condition_items.empty?
    end

    conditions.compact.uniq
  end

  protected

  def conditions_from_join_clauses(from_clause)
    condition_items = []
    from_clause.each do |item|
      next unless item[JOIN_EXPR]

      joinexpr_items = [item[JOIN_EXPR]]
      loop do
        next_item = joinexpr_items.shift
        break unless next_item
        condition_items << next_item['quals'] if next_item['quals']
        %w(larg rarg).each do |side|
          next unless next_item[side][JOIN_EXPR]
          joinexpr_items << next_item[side][JOIN_EXPR]
        end
      end
    end
    condition_items
  end
end
