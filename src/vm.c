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
  Cursor *cursor;
  if (statement->has_where && strcmp(statement->where_column, "id") == 0) {
    uint32_t id = (uint32_t)atoi(statement->where_value);
    cursor = table_find(table, id);
  } else {
    cursor = table_start(table);
  }

  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);

    int match = 1;
    if (statement->has_where) {
      if (strcmp(statement->where_column, "id") == 0) {
        uint32_t id = (uint32_t)atoi(statement->where_value);
        if (row.id != id)
          match = 0;

        // If we used index scan and found a mismatch (or end of table), we can
        // stop But table_find returns the position where key *should* be. If
        // key is unique, we only check this one cell. If we want to support
        // non-unique keys later, we'd continue. For now, if we did index scan,
        // we can just check this one row and break.
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

    // If we are doing index lookup on ID, we only need to check one row
    if (statement->has_where && strcmp(statement->where_column, "id") == 0) {
      break;
    }

    cursor_advance(cursor);
  }
  free(cursor);
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_delete(Statement *statement, Table *table) {
  Cursor *cursor;
  if (statement->has_where && strcmp(statement->where_column, "id") == 0) {
    uint32_t id = (uint32_t)atoi(statement->where_value);
    cursor = table_find(table, id);

    Row row;
    deserialize_row(cursor_value(cursor), &row);
    if (row.id == id) {
      leaf_node_delete(cursor, id);
      printf("Deleted row with id %d\n", id);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
  }

  // Full table scan delete (not optimized, and tricky with cursor)
  // For now, only support ID-based delete or simple delete all (if we implement
  // it carefully) But the task said "DELETE Support", implying WHERE clause. If
  // WHERE is not ID, we need full scan.

  cursor = table_start(table);
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
      leaf_node_delete(cursor, row.id);
      printf("Deleted row with id %d\n", row.id);
      // Do NOT advance cursor, because next row shifted into current slot
      // But we need to check if we are at end of leaf?
      // leaf_node_delete decrements num_cells.
      // If we are at the last cell and delete it, num_cells decreases, so
      // cursor->cell_num might be == num_cells. In that case, we need to
      // advance to next leaf? But cursor_advance handles cell_num >= num_cells.
      // So we can just check if we are at end.
      // Actually, if we delete, the current cell is now the *next* row.
      // So we should process the *same* cell_num again.
      // But we need to check if we reached the end of the node.

      // Simple hack: Close and reopen cursor? No, that's inefficient.
      // Let's just NOT advance. But we need to check if we need to jump to next
      // leaf. If cell_num == num_cells (after delete), we need to advance.

      void *node = get_page(table->pager, cursor->page_num);
      uint32_t num_cells = *leaf_node_num_cells(node);
      if (cursor->cell_num >= num_cells) {
        // We deleted the last cell in this node.
        // We need to advance to next leaf.
        // But cursor_advance increments cell_num first.
        // We want to go to next leaf, cell 0.

        uint32_t next_page_num = *leaf_node_next_leaf(node);
        if (next_page_num == 0) {
          cursor->end_of_table = true;
        } else {
          cursor->page_num = next_page_num;
          cursor->cell_num = 0;
        }
      }
      // Else: stay at current cell_num, which now holds the next row.
    } else {
      cursor_advance(cursor);
    }
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
  case STATEMENT_DELETE:
    return execute_delete(statement, table);
  }
  return EXECUTE_SUCCESS;
}
