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
    int args_assigned = sscanf(
        input_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id),
        statement->row_to_insert.username, statement->row_to_insert.email);
    if (args_assigned < 3) {
      return PREPARE_SYNTAX_ERROR;
    }
    return PREPARE_SUCCESS;
  }
  if (strncmp(input_buffer->buffer, "select", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    statement->has_where = 0;

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

  return PREPARE_UNRECOGNIZED_STATEMENT;
}
