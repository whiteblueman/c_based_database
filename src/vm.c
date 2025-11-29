#include "vm.h"
#include "cursor.h"
#include "node.h"
#include "table.h"
#include <stdio.h>
#include <stdlib.h>

ExecuteResult execute_insert(Statement *statement, Table *table) {
  void *node = get_page(table->pager, 0);
  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    return EXECUTE_TABLE_FULL;
  }

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
    printf("(%d, %s, %s)\n", row.id, row.username, row.email);
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
