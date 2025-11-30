#include "vm.h"
#include "cursor.h"
#include "node.h"
#include "table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void serialize_order_row(OrderRow *source, void *destination) {
  memcpy((char *)destination + ORDER_ID_OFFSET, &(source->id), ORDER_ID_SIZE);
  memcpy((char *)destination + ORDER_USER_ID_OFFSET, &(source->user_id),
         ORDER_USER_ID_SIZE);
  memcpy((char *)destination + ORDER_PRODUCT_NAME_OFFSET,
         &(source->product_name), ORDER_PRODUCT_NAME_SIZE);
}

void deserialize_order_row(void *source, OrderRow *destination) {
  memcpy(&(destination->id), (char *)source + ORDER_ID_OFFSET, ORDER_ID_SIZE);
  memcpy(&(destination->user_id), (char *)source + ORDER_USER_ID_OFFSET,
         ORDER_USER_ID_SIZE);
  memcpy(&(destination->product_name),
         (char *)source + ORDER_PRODUCT_NAME_OFFSET, ORDER_PRODUCT_NAME_SIZE);
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
  if (strcmp(statement->table_name, "orders") == 0) {
    void *node = get_page(table->pager, table->orders_root_page_num);
    uint32_t num_cells =
        *leaf_node_num_cells(node); // Assuming root is leaf for now

    Cursor *cursor = table_end(table, table->orders_root_page_num);

    // We need to define leaf_node_insert for orders?
    // leaf_node_insert is generic but takes key_size and value_size.
    // For orders: key is ID (int), value is user_id + product_name?
    // Or just store full row as value?
    // Let's assume Key = ID, Value = Row (like users).

    leaf_node_insert(cursor, &(statement->order_to_insert.id), sizeof(uint32_t),
                     &(statement->order_to_insert), sizeof(OrderRow), KEY_INT);

    free(cursor);
    return EXECUTE_SUCCESS;
  } else {
    // Users table
    void *node = get_page(table->pager, table->main_root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor *cursor = table_end(table, table->main_root_page_num);

    leaf_node_insert(cursor, &(statement->row_to_insert.id),
                     MAIN_TABLE_KEY_SIZE, &(statement->row_to_insert),
                     MAIN_TABLE_VALUE_SIZE, KEY_INT);

    // Insert into Secondary Index
    Cursor *index_cursor = table_find(table, table->index_root_page_num,
                                      statement->row_to_insert.username,
                                      USERNAME_INDEX_KEY_SIZE, KEY_STRING);

    // Check for duplicates? For now, just insert.
    leaf_node_insert(index_cursor, statement->row_to_insert.username,
                     USERNAME_INDEX_KEY_SIZE, &(statement->row_to_insert.id),
                     USERNAME_INDEX_VALUE_SIZE, KEY_STRING);

    free(cursor);
    free(index_cursor);

    return EXECUTE_SUCCESS;
  }
}

void print_row(Row *row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

ExecuteResult execute_select(Statement *statement, Table *table) {
  if (statement->has_join) {
    // Nested Loop Join
    // Outer: Users (default)
    // Inner: Orders

    Cursor *outer_cursor = table_start(table, table->main_root_page_num);

    while (!(outer_cursor->end_of_table)) {
      Row outer_row;
      void *outer_val = cursor_value(outer_cursor);
      deserialize_row(outer_val, &outer_row);

      // Inner Loop: Orders
      Cursor *inner_cursor = table_start(table, table->orders_root_page_num);
      while (!(inner_cursor->end_of_table)) {
        void *page = get_page(table->pager, inner_cursor->page_num);

        uint32_t key_size = sizeof(uint32_t);
        uint32_t value_size = sizeof(OrderRow);
        uint32_t cell_size = key_size + value_size;

        void *inner_val =
            leaf_node_value(page, inner_cursor->cell_num, cell_size, key_size);
        OrderRow inner_row;
        deserialize_order_row(inner_val, &inner_row);

        // Check Join Condition: users.id = orders.user_id
        if (outer_row.id == inner_row.user_id) {
          printf("(%d, %s, %s) | (%d, %d, %s)\n", outer_row.id,
                 outer_row.username, outer_row.email, inner_row.id,
                 inner_row.user_id, inner_row.product_name);
        }

        cursor_advance(inner_cursor);
      }
      free(inner_cursor);

      cursor_advance(outer_cursor);
    }
    free(outer_cursor);
    return EXECUTE_SUCCESS;
  }

  if (statement->has_where &&
      strcmp(statement->where_column, "username") == 0) {
    // Index Scan
    Cursor *index_cursor =
        table_find(table, table->index_root_page_num, statement->where_value,
                   USERNAME_INDEX_KEY_SIZE, KEY_STRING);

    void *node = get_page(table->pager, index_cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    if (index_cursor->cell_num < num_cells) {
      void *key = leaf_node_key(node, index_cursor->cell_num,
                                USERNAME_INDEX_LEAF_CELL_SIZE);
      if (compare_keys(key, statement->where_value, KEY_STRING,
                       USERNAME_INDEX_KEY_SIZE) == 0) {
        // Found match, get Primary Key
        uint32_t *user_id_ptr = (uint32_t *)leaf_node_value(
            node, index_cursor->cell_num, USERNAME_INDEX_LEAF_CELL_SIZE,
            USERNAME_INDEX_KEY_SIZE);
        uint32_t user_id = *user_id_ptr;

        // Lookup in Main Table by ID
        Cursor *main_cursor =
            table_find(table, table->main_root_page_num, &user_id,
                       MAIN_TABLE_KEY_SIZE, KEY_INT);

        // Verify we found it
        Row row;
        deserialize_row(cursor_value(main_cursor), &row);
        print_row(&row);

        free(main_cursor);
      }
    }
    free(index_cursor);
    return EXECUTE_SUCCESS;
  }

  // Full Table Scan (Main Table)
  Cursor *cursor = table_start(table, table->main_root_page_num);
  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);

    int match = 1;
    if (statement->has_where) {
      // Handle ID filter (int)
      if (strcmp(statement->where_column, "id") == 0) {
        int val = atoi(statement->where_value);
        if (row.id != val)
          match = 0;
      }
      // Handle Username filter (string)
      else if (strcmp(statement->where_column, "username") == 0) {
        if (strcmp(row.username, statement->where_value) != 0)
          match = 0;
      }
    }

    if (match) {
      print_row(&row);
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
    cursor = table_find(table, table->main_root_page_num, &id,
                        MAIN_TABLE_KEY_SIZE, KEY_INT);

    Row row;
    deserialize_row(cursor_value(cursor), &row);
    if (row.id == id) {
      // Delete from Main Table
      leaf_node_delete(cursor, &id, MAIN_TABLE_KEY_SIZE, KEY_INT);

      // Delete from Index
      Cursor *index_cursor =
          table_find(table, table->index_root_page_num, row.username,
                     USERNAME_INDEX_KEY_SIZE, KEY_STRING);
      leaf_node_delete(index_cursor, row.username, USERNAME_INDEX_KEY_SIZE,
                       KEY_STRING);
      free(index_cursor);

      printf("Deleted row with id %d\n", id);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
  }

  // Full table scan delete
  cursor = table_start(table, table->main_root_page_num);
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
      // Delete from Main Table
      leaf_node_delete(cursor, &(row.id), MAIN_TABLE_KEY_SIZE, KEY_INT);

      // Delete from Index
      Cursor *index_cursor =
          table_find(table, table->index_root_page_num, row.username,
                     USERNAME_INDEX_KEY_SIZE, KEY_STRING);
      leaf_node_delete(index_cursor, row.username, USERNAME_INDEX_KEY_SIZE,
                       KEY_STRING);
      free(index_cursor);

      printf("Deleted row with id %d\n", row.id);

      void *node = get_page(table->pager, cursor->page_num);
      uint32_t num_cells = *leaf_node_num_cells(node);
      if (cursor->cell_num >= num_cells) {
        uint32_t next_page_num = *leaf_node_next_leaf(node);
        if (next_page_num == 0) {
          cursor->end_of_table = true;
        } else {
          cursor->page_num = next_page_num;
          cursor->cell_num = 0;
        }
      }
    } else {
      cursor_advance(cursor);
    }
  }
  free(cursor);
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_insert_select(Statement *statement, Table *table) {
  // Hardcoded logic for: INSERT INTO orders SELECT ... FROM users
  if (strcmp(statement->table_name, "orders") == 0 &&
      strcmp(statement->select_source_table, "users") == 0) {

    Cursor *source_cursor = table_start(table, table->main_root_page_num);

    while (!(source_cursor->end_of_table)) {
      Row source_row;
      deserialize_row(cursor_value(source_cursor), &source_row);

      int match = 1;
      if (statement->select_has_where) {
        // Simple string match on username for now
        if (strcmp(statement->select_where_column, "username") == 0) {
          if (strcmp(source_row.username, statement->select_where_value) != 0)
            match = 0;
        }
      }

      if (match) {
        // Create Order Row
        OrderRow order;
        order.id = source_row.id + 1000; // Offset to distinguish
        order.user_id = source_row.id;
        strcpy(order.product_name, "AutoImport");

        // Insert into Orders
        Cursor *target_cursor = table_end(table, table->orders_root_page_num);
        leaf_node_insert(target_cursor, &(order.id), sizeof(uint32_t), &order,
                         sizeof(OrderRow), KEY_INT);
        free(target_cursor);

        printf("Inserted Order %d for User %d\n", order.id, order.user_id);
      }

      cursor_advance(source_cursor);
    }
    free(source_cursor);
    return EXECUTE_SUCCESS;
  }
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_begin(Statement *statement, Table *table) {
  if (table->in_transaction) {
    printf("Error: Already in a transaction\n");
    return EXECUTE_SUCCESS; // Or error?
  }
  table->in_transaction = true;
  printf("Transaction started.\n");
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_commit(Statement *statement, Table *table) {
  if (!table->in_transaction) {
    printf("Error: Not in a transaction\n");
    return EXECUTE_SUCCESS;
  }

  // Flush all pages to disk
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    if (table->pager->pages[i] != NULL) {
      pager_flush(table->pager, i, PAGE_SIZE);
    }
  }

  table->in_transaction = false;
  printf("Transaction committed.\n");
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_rollback(Statement *statement, Table *table) {
  if (!table->in_transaction) {
    printf("Error: Not in a transaction\n");
    return EXECUTE_SUCCESS;
  }

  pager_rollback(table->pager);

  // We need to reload root pages because pointers might be invalid
  // But get_page handles reloading.
  // However, table->main_root_page_num etc are just numbers.
  // The actual pointers in Cursors might be invalid if we had open cursors?
  // VM executes one statement at a time, so no open cursors across statements
  // usually.

  table->in_transaction = false;
  printf("Transaction rolled back.\n");
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
  case STATEMENT_INSERT_SELECT:
    return execute_insert_select(statement, table);
  case STATEMENT_BEGIN:
    return execute_begin(statement, table);
  case STATEMENT_COMMIT:
    return execute_commit(statement, table);
  case STATEMENT_ROLLBACK:
    return execute_rollback(statement, table);
  }
  return EXECUTE_SUCCESS;
}
