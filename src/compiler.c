#include "compiler.h"
#include "table.h"
#include <stdio.h>
#include <string.h>

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    db_close(table);
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    statement->type = STATEMENT_INSERT;
    // Check if it's "insert into orders ..." or just "insert ..." (default
    // users) Actually, let's support "insert into table ..." syntax or just
    // "insert table ..." The previous syntax was "insert id username email".
    // Let's try to detect "insert into orders" or "insert orders".

    char *keyword_into = strstr(input_buffer->buffer, "into");
    char *table_start = input_buffer->buffer + 7;
    if (keyword_into) {
      table_start = keyword_into + 5;
    }

    // Check for INSERT INTO ... SELECT ...
    // Example: INSERT INTO orders SELECT ...
    if (strstr(input_buffer->buffer, "select") != NULL) {
      statement->type = STATEMENT_INSERT_SELECT;

      // Parse target table
      char *into_ptr = strstr(input_buffer->buffer, "into");
      char *select_ptr = strstr(input_buffer->buffer, "select");

      if (into_ptr && select_ptr && into_ptr < select_ptr) {
        // Extract table name between "into" and "select"
        // "insert into orders select ..."
        sscanf(into_ptr, "into %s", statement->table_name);

        // Parse Source Table from SELECT part
        // "select ... from users ..."
        char *from_ptr = strstr(select_ptr, "from");
        if (from_ptr) {
          sscanf(from_ptr, "from %s", statement->select_source_table);

          // Parse WHERE in SELECT part
          char *where_ptr = strstr(from_ptr, "where");
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
      }
      return PREPARE_SYNTAX_ERROR;
    }

    // Simple parsing: check if "orders" is present early on
    if (strstr(input_buffer->buffer, "orders") != NULL) {
      strcpy(statement->table_name, "orders");
      // Format: insert into orders id user_id product_name
      // or: insert orders id user_id product_name
      // Skip "insert into orders" or "insert orders"
      char *args = strstr(input_buffer->buffer, "orders") + 6;
      int args_assigned =
          sscanf(args, "%d %d %s", &(statement->order_to_insert.id),
                 &(statement->order_to_insert.user_id),
                 statement->order_to_insert.product_name);
      if (args_assigned < 3) {
        return PREPARE_SYNTAX_ERROR;
      }
      return PREPARE_SUCCESS;
    } else {
      // Default to users table
      strcpy(statement->table_name, "users");
      // Format: insert id username email
      // Skip "insert"
      char *args = input_buffer->buffer + 6;
      // If "into users" is used, skip that too
      if (strstr(input_buffer->buffer, "users") != NULL) {
        args = strstr(input_buffer->buffer, "users") + 5;
      }

      int args_assigned = sscanf(
          args, "%d %s %s", &(statement->row_to_insert.id),
          statement->row_to_insert.username, statement->row_to_insert.email);
      if (args_assigned < 3) {
        return PREPARE_SYNTAX_ERROR;
      }
      return PREPARE_SUCCESS;
    }
  }
  if (strncmp(input_buffer->buffer, "select", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    statement->has_where = 0;
    statement->has_join = 0;
    strcpy(statement->table_name, "users"); // Default

    // Check for JOIN
    char *join_ptr = strstr(input_buffer->buffer, "join");
    if (join_ptr != NULL) {
      statement->has_join = 1;
      // Assume syntax: select ... from users join orders on users.id =
      // orders.user_id Or simplified: select join orders on users.id =
      // orders.user_id Let's parse "join <table_name> on <left> = <right>"

      sscanf(join_ptr, "join %s on %s = %s", statement->join_table_name,
             statement->join_condition_left, statement->join_condition_right);
    }

    char *where_ptr = strstr(input_buffer->buffer, "where");
    if (where_ptr != NULL) {
      int args_assigned =
          sscanf(where_ptr, "where %s %s %s", statement->where_column,
                 statement->where_operator, statement->where_value);
      if (args_assigned == 3) {
        statement->has_where = 1;
      } else {
        return PREPARE_SYNTAX_ERROR;
      }
    }
    return PREPARE_SUCCESS;
  }

  if (strncmp(input_buffer->buffer, "delete", 6) == 0) {
    statement->type = STATEMENT_DELETE;
    statement->has_where = 0;
    strcpy(statement->table_name, "users"); // Default

    char *where_ptr = strstr(input_buffer->buffer, "where");
    if (where_ptr != NULL) {
      int args_assigned =
          sscanf(where_ptr, "where %s %s %s", statement->where_column,
                 statement->where_operator, statement->where_value);
      if (args_assigned == 3) {
        statement->has_where = 1;
      } else {
        return PREPARE_SYNTAX_ERROR;
      }
    } else {
      // DELETE without WHERE is dangerous, but we'll allow it (delete all)
      // Or maybe we should require WHERE for now to be safe/simple?
      // Let's allow it, it just means delete all rows.
    }
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}
