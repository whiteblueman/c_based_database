#include "compiler.h"
#include "table.h"
#include <stdio.h>
#include <string.h>

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table,
                                  int out_fd) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
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
  // printf("Debug: prepare_statement buffer: '%s' (Hex: %02x %02x %02x)\n",
  // input_buffer->buffer, input_buffer->buffer[0], input_buffer->buffer[1],
  // input_buffer->buffer[2]); INSERT
  if (strncmp(input_buffer->buffer, "insert", 6) == 0 ||
      strncmp(input_buffer->buffer, "INSERT", 6) == 0) {
    statement->type = STATEMENT_INSERT;

    // Check for "INSERT INTO"
    char *into_ptr = strcasestr(input_buffer->buffer, "into");
    if (into_ptr) {
      // ANSI SQL: INSERT INTO <table> VALUES (...)
      // Parse table name
      char *table_start = into_ptr + 5;
      char table_name[32];
      sscanf(table_start, "%s", table_name);
      strcpy(statement->table_name, table_name);

      // Check for VALUES
      char *values_ptr = strcasestr(input_buffer->buffer, "values");
      if (values_ptr) {
        // Parse values inside parentheses
        char *paren_start = strchr(values_ptr, '(');
        if (paren_start) {
          if (strcmp(table_name, "orders") == 0) {
            // INSERT INTO orders VALUES (id, user_id, 'product')
            // Note: sscanf with %[^']' reads until single quote
            int id, user_id;
            char product[255];
            // Try parsing: (100, 1, 'Apple')
            // We need to handle quotes.
            // Simplified parsing: assume format (id, user_id, 'product')
            // Skip '(', read int, comma, int, comma, quote, string, quote, ')'
            // Or just use sscanf with format
            int assigned = sscanf(paren_start, "(%d, %d, '%[^']')", &id,
                                  &user_id, product);
            if (assigned < 3) {
              // Try without quotes?
              assigned = sscanf(paren_start, "(%d, %d, %[^)])", &id, &user_id,
                                product);
            }

            if (assigned < 3)
              return PREPARE_SYNTAX_ERROR;

            statement->order_to_insert.id = id;
            statement->order_to_insert.user_id = user_id;
            strcpy(statement->order_to_insert.product_name, product);
            return PREPARE_SUCCESS;

          } else {
            // Default users: INSERT INTO users VALUES (id, 'username', 'email')
            int id;
            char username[255];
            char email[255];

            // Try parsing: (1, 'user1', 'email1')
            int assigned = sscanf(paren_start, "(%d, '%[^']', '%[^']')", &id,
                                  username, email);
            if (assigned < 3) {
              // Try without quotes
              assigned = sscanf(paren_start, "(%d, %[^,], %[^)])", &id,
                                username, email);
              // Trim spaces if needed? sscanf %s skips spaces but %[^,] might
              // not
            }

            if (assigned < 3)
              return PREPARE_SYNTAX_ERROR;

            if (id < 0)
              return PREPARE_NEGATIVE_ID;
            if (strlen(username) > 32)
              return PREPARE_STRING_TOO_LONG;
            if (strlen(email) > 255)
              return PREPARE_STRING_TOO_LONG;

            statement->row_to_insert.id = id;
            strcpy(statement->row_to_insert.username, username);
            strcpy(statement->row_to_insert.email, email);
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

    // Fallback to old syntax: insert 1 user1 email
    // ... (Keep existing logic for backward compatibility or remove?)
    // Let's keep it for now but maybe prioritize ANSI

    // Existing logic for "insert orders ..." or "insert ..."
    // ...
    // Copy-paste existing logic here or refactor.
    // For brevity in this edit, I will include the old logic as fallback.

    // Simple parsing: check if "orders" is present early on
    if (strstr(input_buffer->buffer, "orders") != NULL) {
      strcpy(statement->table_name, "orders");
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
      strcpy(statement->table_name, "users");
      char *args = input_buffer->buffer + 6;
      if (strstr(input_buffer->buffer, "users") != NULL) {
        args = strstr(input_buffer->buffer, "users") + 5;
      }

      int id;
      char username[255];
      char email[255];

      int args_assigned = sscanf(args, "%d %s %s", &id, username, email);
      if (args_assigned < 3) {
        return PREPARE_SYNTAX_ERROR;
      }

      if (id < 0)
        return PREPARE_NEGATIVE_ID;
      if (strlen(username) > 32)
        return PREPARE_STRING_TOO_LONG;
      if (strlen(email) > 255)
        return PREPARE_STRING_TOO_LONG;

      statement->row_to_insert.id = id;
      strcpy(statement->row_to_insert.username, username);
      strcpy(statement->row_to_insert.email, email);

      return PREPARE_SUCCESS;
    }
  }

  // SELECT
  if (strncmp(input_buffer->buffer, "select", 6) == 0 ||
      strncmp(input_buffer->buffer, "SELECT", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    statement->has_where = 0;
    statement->has_join = 0;
    strcpy(statement->table_name, "users"); // Default

    // ANSI SQL: SELECT * FROM <table>
    char *from_ptr = strcasestr(input_buffer->buffer, "from");
    if (from_ptr) {
      char *args = from_ptr + 4; // Skip "from"
      sscanf(args, "%s", statement->table_name);
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

  if (strncmp(input_buffer->buffer, "create table", 12) == 0) {
    statement->type = STATEMENT_CREATE_TABLE;
    char *args = input_buffer->buffer + 12;
    // Format: create table <name> (<cols>)
    // Example: create table my_users (id int, username varchar(32), email
    // varchar(255))

    char table_name[32];
    sscanf(args, "%s", table_name);
    strcpy(statement->create_table_name, table_name);

    // Determine schema type by checking columns
    // Simplistic check: if contains "product_name" -> Order, else User
    if (strstr(input_buffer->buffer, "product_name")) {
      statement->create_schema_type = 1; // Order
    } else {
      statement->create_schema_type = 0; // User
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
