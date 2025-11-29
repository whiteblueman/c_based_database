#include "vm.h"
#include "cursor.h"
#include "node.h"
#include "table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

ExecuteResult execute_insert(Statement *statement, Table *table) {

  Row *row_to_insert = &(statement->row_to_insert);
  Cursor *cursor = table_end(table);

  // For now, we just append to the end.
  // In a real B-Tree, we would search for the correct position.
  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table) {
  Cursor *cursor = table_start(table);
  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);

    int match = 1;
    if (statement->has_where) {
      if (strcmp(statement->where_column, "id") == 0) {
        uint32_t id = (uint32_t)atoi(statement->where_value);
        if (row.id != id)
          match = 0;
      } else if (strcmp(statement->where_column, "username") == 0) {
        if (strcmp(row.username, statement->where_value) != 0)
          match = 0;
      } else if (strcmp(statement->where_column, "email") == 0) {
        if (strcmp(row.email, statement->where_value) != 0)
          match = 0;
      }
    }

    if (match) {
      printf("(%d, %s, %s)\n", row.id, row.username, row.email);
    }
    cursor_advance(cursor);
  }
  free(cursor);
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table) {
  switch (statement->type) {
  case STATEMENT_INSERT:
    return execute_insert(statement, table);
  case STATEMENT_SELECT:
    return execute_select(statement, table);
  }
  return EXECUTE_SUCCESS;
}
