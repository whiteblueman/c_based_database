#include "compiler.h"
#include "table.h"
#include <stdio.h>
#include <string.h>

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table,
                                  int out_fd) {
  if (strcmp(input_buffer->buffer, ".exit") == 0 ||
      strcmp(input_buffer->buffer, "exit") == 0) {
    close_input_buffer(input_buffer);
    db_close(table);
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".tables") == 0) {
    dprintf(out_fd, "users\norders\n");
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".schema") == 0) {
    dprintf(
        out_fd,
        "CREATE TABLE users (\n    id integer,\n    username varchar(32),\n    "
        "email varchar(255)\n);\nCREATE TABLE orders (\n    id integer,\n    "
        "user_id integer,\n    product_name varchar(32)\n);\n");
    return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  if (strncmp(input_buffer->buffer, "create table", 12) == 0 ||
      strncmp(input_buffer->buffer, "CREATE TABLE", 12) == 0) {
    statement->type = STATEMENT_CREATE_TABLE;
    char *args = input_buffer->buffer + 12;
    // Parse table name
    char *paren = strchr(args, '(');
    if (!paren) {
      printf("Debug: No opening parenthesis found\n");
      return PREPARE_SYNTAX_ERROR;
    }

    int name_len = paren - args;
    while (name_len > 0 && args[name_len - 1] == ' ')
      name_len--; // Trim trailing space
    while (*args == ' ') {
      args++;
      name_len--;
    } // Trim leading space

    if (name_len >= 32)
      return PREPARE_STRING_TOO_LONG;
    strncpy(statement->create_table_name, args, name_len);
    statement->create_table_name[name_len] = '\0';

    // Parse columns
    char *cols_start = paren + 1;
    char *cols_end = strrchr(cols_start, ')');
    if (!cols_end)
      return PREPARE_SYNTAX_ERROR;
    *cols_end = '\0'; // Terminate string at closing paren

    statement->create_num_columns = 0;
    char *token = strtok(cols_start, ",");
    while (token != NULL) {
      if (statement->create_num_columns >= 10)
        break;

      char col_name[32];
      char col_type[32];
      sscanf(token, "%s %s", col_name, col_type);

      strcpy(statement->create_column_names[statement->create_num_columns],
             col_name);
      if (strcasecmp(col_type, "int") == 0 ||
          strcasecmp(col_type, "integer") == 0) {
        statement->create_column_types[statement->create_num_columns] =
            0; // INT
      } else {
        statement->create_column_types[statement->create_num_columns] =
            1; // VARCHAR
      }
      statement->create_num_columns++;
      token = strtok(NULL, ",");
    }

    // Legacy support for schema_type (optional, can be removed if VM uses
    // columns)
    statement->create_schema_type = 0;

    return PREPARE_SUCCESS;
  }

  if (strncasecmp(input_buffer->buffer, "show tables", 11) == 0) {
    statement->type = STATEMENT_SHOW_TABLES;
    return PREPARE_SUCCESS;
  }

  if (strncmp(input_buffer->buffer, "desc", 4) == 0 ||
      strncmp(input_buffer->buffer, "DESC", 4) == 0) {
    statement->type = STATEMENT_DESC_TABLE;
    char *args = input_buffer->buffer + 4;
    while (*args == ' ')
      args++; // Skip whitespace
    if (*args == '\0')
      return PREPARE_SYNTAX_ERROR;

    sscanf(args, "%s", statement->desc_table_name);
    return PREPARE_SUCCESS;
  }

  if (strncasecmp(input_buffer->buffer, "show index from", 15) == 0) {
    statement->type = STATEMENT_SHOW_INDEX;
    char *args = input_buffer->buffer + 15;
    while (*args == ' ')
      args++;
    sscanf(args, "%s", statement->desc_table_name); // Reuse desc_table_name
    return PREPARE_SUCCESS;
  }

  if (strncmp(input_buffer->buffer, "insert", 6) == 0 ||
      strncmp(input_buffer->buffer, "INSERT", 6) == 0) {
    statement->type = STATEMENT_INSERT;

    // Check for "INSERT INTO"
    char *into_ptr = strcasestr(input_buffer->buffer, "into");
    if (into_ptr) {
      // ANSI SQL: INSERT INTO <table> VALUES (...)
      // Check for VALUES first to define boundary
      char *values_ptr = strcasestr(into_ptr, "values");
      if (values_ptr) {
        // Parse table name between "into" and "values"
        char *table_start = into_ptr + 4;
        int name_len = values_ptr - table_start;

        // Trim spaces
        while (name_len > 0 && table_start[name_len - 1] == ' ')
          name_len--;
        while (*table_start == ' ') {
          table_start++;
          name_len--;
        }

        char table_name[32];
        if (name_len >= 32)
          return PREPARE_STRING_TOO_LONG;
        strncpy(table_name, table_start, name_len);
        table_name[name_len] = '\0';

        strcpy(statement->table_name, table_name);

        // Parse values inside parentheses
        char *paren_start = strchr(values_ptr, '(');
        if (paren_start) {
          // Dynamic parsing for any table
          // Format: (val1, val2, ...)
          char *vals = paren_start + 1;
          int val_idx = 0;
          char *token = strtok(vals, ",)");
          while (token != NULL) {
            if (val_idx >= 10)
              break; // Limit to 10 columns for now

            // Trim spaces
            while (*token == ' ')
              token++;

            // Handle quotes
            if (*token == '\'') {
              token++; // Skip opening quote
              char *quote_end = strchr(token, '\'');
              if (quote_end)
                *quote_end = '\0';
            }

            // Store in statement
            statement->insert_values[val_idx++] = token;

            token = strtok(NULL, ",)");
          }
          statement->insert_values[val_idx] = NULL; // Null terminate array
          return PREPARE_SUCCESS;
        }
      }
    }

    // Check for SELECT (INSERT INTO ... SELECT ...)
    if (strcasestr(input_buffer->buffer, "select")) {
      statement->type = STATEMENT_INSERT_SELECT;
      // Parse target table already done above

      // Parse Source Table
      char *select_ptr = strcasestr(input_buffer->buffer, "select");
      char *from_ptr = strcasestr(select_ptr, "from");
      if (from_ptr) {
        sscanf(from_ptr, "from %s", statement->select_source_table);

        char *where_ptr = strcasestr(from_ptr, "where");
        if (where_ptr) {
          statement->select_has_where = 1;
          sscanf(where_ptr, "where %s %s %s", statement->select_where_column,
                 statement->select_where_operator,
                 statement->select_where_value);
        } else {
          statement->select_has_where = 0;
        }
        return PREPARE_SUCCESS;
      }
      return PREPARE_SYNTAX_ERROR;
    }
  }

  // SELECT
  if (strncmp(input_buffer->buffer, "select", 6) == 0 ||
      strncmp(input_buffer->buffer, "SELECT", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    statement->has_where = 0;
    statement->has_join = 0;
    statement->limit = -1;
    statement->num_select_columns = 0;
    strcpy(statement->table_name, "users"); // Default

    // ANSI SQL: SELECT <columns> FROM <table>
    char *from_ptr = strcasestr(input_buffer->buffer, "from");
    if (from_ptr) {
      char *args = from_ptr + 4;
      sscanf(args, "%s", statement->table_name);

      // Parse columns between SELECT and FROM
      char *select_ptr = input_buffer->buffer + 6; // Skip "select" (or SELECT)
      int len = from_ptr - select_ptr;
      char cols_str[256];
      if (len < 256) {
        strncpy(cols_str, select_ptr, len);
        cols_str[len] = '\0';

        // Check for *
        if (strstr(cols_str, "*") == NULL) {
          char *token = strtok(cols_str, ",");
          while (token != NULL) {
            // Trim spaces
            while (*token == ' ')
              token++;
            char *end = token + strlen(token) - 1;
            while (end > token && *end == ' ')
              *end-- = '\0';

            if (strlen(token) > 0) {
              strcpy(statement->select_columns[statement->num_select_columns++],
                     token);
              if (statement->num_select_columns >= 10)
                break;
            }
            token = strtok(NULL, ",");
          }
        }
      }
    }

    // Check for JOIN
    char *join_ptr = strcasestr(input_buffer->buffer, "join");
    if (join_ptr != NULL) {
      statement->has_join = 1;
      char *args = join_ptr + 4; // Skip "join"
      // We need to parse "<table> on <left> = <right>"
      // "on" might be mixed case too.
      // Let's just use sscanf with %s and hope "on" is handled or skip it.
      // sscanf(args, "%s on %s = %s") expects "on".
      // Better: parse table, then find "on".

      char table_name[32];
      sscanf(args, "%s", table_name);
      strcpy(statement->join_table_name, table_name);

      char *on_ptr = strcasestr(args, "on");
      if (on_ptr) {
        char *on_args = on_ptr + 2; // Skip "on"
        sscanf(on_args, "%s = %s", statement->join_condition_left,
               statement->join_condition_right);
      }
    }

    char *where_ptr = strcasestr(input_buffer->buffer, "where");
    if (where_ptr != NULL) {
      char *args = where_ptr + 5; // Skip "where"
      int args_assigned =
          sscanf(args, "%s %s %s", statement->where_column,
                 statement->where_operator, statement->where_value);
      if (args_assigned == 3) {
        statement->has_where = 1;
        // Strip semicolon from value if present
        char *semicolon = strchr(statement->where_value, ';');
        if (semicolon)
          *semicolon = '\0';
      } else {
        return PREPARE_SYNTAX_ERROR;
      }
    }

    // Check for LIMIT
    statement->limit = -1; // Default no limit
    char *limit_ptr = strcasestr(input_buffer->buffer, "limit");
    if (limit_ptr != NULL) {
      char *args = limit_ptr + 5; // Skip "limit"
      int limit_val;
      if (sscanf(args, "%d", &limit_val) == 1) {
        statement->limit = limit_val;
      }
    }
    return PREPARE_SUCCESS;
  }

  // DELETE
  if (strncmp(input_buffer->buffer, "delete", 6) == 0 ||
      strncmp(input_buffer->buffer, "DELETE", 6) == 0) {
    statement->type = STATEMENT_DELETE;
    statement->has_where = 0;
    strcpy(statement->table_name, "users"); // Default

    // ANSI SQL: DELETE FROM <table>
    char *from_ptr = strcasestr(input_buffer->buffer, "from");
    if (from_ptr) {
      char *args = from_ptr + 4;
      sscanf(args, "%s", statement->table_name);
    }

    char *where_ptr = strcasestr(input_buffer->buffer, "where");
    if (where_ptr != NULL) {
      char *args = where_ptr + 5;
      int args_assigned =
          sscanf(args, "%s %s %s", statement->where_column,
                 statement->where_operator, statement->where_value);
      if (args_assigned == 3) {
        statement->has_where = 1;
        // Strip semicolon
        char *semicolon = strchr(statement->where_value, ';');
        if (semicolon)
          *semicolon = '\0';
      } else {
        return PREPARE_SYNTAX_ERROR;
      }
    }
    return PREPARE_SUCCESS;
  }

  if (strncmp(input_buffer->buffer, "begin", 5) == 0) {
    statement->type = STATEMENT_BEGIN;
    return PREPARE_SUCCESS;
  }
  if (strncmp(input_buffer->buffer, "commit", 6) == 0) {
    statement->type = STATEMENT_COMMIT;
    return PREPARE_SUCCESS;
  }
  if (strncmp(input_buffer->buffer, "rollback", 8) == 0) {
    statement->type = STATEMENT_ROLLBACK;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}
