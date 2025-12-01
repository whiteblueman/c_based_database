#include "vm.h"
#include "cursor.h"
#include "node.h"
#include "table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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

// Helper to print to fd
void print_msg(int fd, const char *msg) { write(fd, msg, strlen(msg)); }

void print_row(Row *row, int out_fd) {
  dprintf(out_fd, "(%d, %s, %s)\n", row->id, row->username, row->email);
}

ExecuteResult execute_insert(Statement *statement, Table *table, int out_fd) {
  TableInfo *table_info = find_table(table, statement->table_name);
  if (table_info == NULL) {
    dprintf(out_fd, "Error: Table '%s' not found.\n", statement->table_name);
    return EXECUTE_TABLE_FULL; // Reuse error code or add new one
  }

  void *node = get_page(table->pager, table_info->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  // Dynamic Serialization
  // Calculate total row size
  uint32_t row_size = 0;
  for (uint32_t i = 0; i < table_info->num_columns; i++) {
    row_size += table_info->columns[i].size;
  }

  // Allocate buffer for row
  char *row_data = malloc(row_size);
  memset(row_data, 0, row_size);

  uint32_t key_to_insert = 0; // Default key is first column if INT

  // Validate number of values
  uint32_t num_values = 0;
  while (statement->insert_values[num_values] != NULL) {
    num_values++;
  }

  // Auto-Increment Logic
  if (num_values == table_info->num_columns - 1 &&
      strcmp(table_info->columns[0].name, "id") == 0 &&
      table_info->columns[0].type == COLUMN_INT) {

    // Find max ID
    Cursor *end_cursor = table_end(table, table_info->root_page_num);
    uint32_t max_id = 0;

    if (end_cursor->cell_num > 0) {
      // Look at the last cell
      uint32_t page_num = end_cursor->page_num;
      void *node = get_page(table->pager, page_num);

      uint32_t last_cell_index = end_cursor->cell_num - 1;
      // Calculate dynamic cell size: key_size (4) + row_size
      uint32_t cell_size = sizeof(uint32_t) + row_size;
      void *key_ptr = leaf_node_key(node, last_cell_index, cell_size);
      memcpy(&max_id, key_ptr, sizeof(uint32_t));
    }
    free(end_cursor);

    key_to_insert = max_id + 1;

    // Prepare row data with new ID
    // Column 0 is ID
    Column *col = &table_info->columns[0];
    memcpy(row_data + col->offset, &key_to_insert, sizeof(uint32_t));

    // Fill other columns from insert_values[0]...
    for (uint32_t i = 1; i < table_info->num_columns; i++) {
      col = &table_info->columns[i];
      char *val_str = statement->insert_values[i - 1]; // Shifted index
      void *dest = row_data + col->offset;

      if (col->type == COLUMN_INT) {
        uint32_t val = atoi(val_str);
        memcpy(dest, &val, sizeof(uint32_t));
      } else {
        strncpy((char *)dest, val_str, col->size);
      }
    }

  } else if (num_values != table_info->num_columns) {
    dprintf(out_fd, "Error: Column count mismatch. Expected %d, got %d.\n",
            table_info->num_columns, num_values);
    free(row_data);
    return EXECUTE_TABLE_FULL; // Reuse error code for now
  } else {
    // Normal Insert
    for (uint32_t i = 0; i < table_info->num_columns; i++) {
      Column *col = &table_info->columns[i];
      char *val_str = statement->insert_values[i];
      void *dest = row_data + col->offset;

      if (col->type == COLUMN_INT) {
        uint32_t val = atoi(val_str);
        memcpy(dest, &val, sizeof(uint32_t));
        if (i == 0)
          key_to_insert = val; // Assume first column is key
      } else {
        strncpy((char *)dest, val_str, col->size);
      }
    }
  }

  Cursor *cursor = table_find(table, table_info->root_page_num, &key_to_insert,
                              sizeof(uint32_t), KEY_INT);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = *(uint32_t *)leaf_node_key(
        cursor_value(cursor), cursor->cell_num, KEY_INT);
    if (key_at_index == key_to_insert) {
      free(row_data);
      free(cursor);
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  leaf_node_insert(cursor, &key_to_insert, sizeof(uint32_t), row_data, row_size,
                   KEY_INT);
  free(row_data);
  free(cursor);

  // Insert into Secondary Index (Username) - ONLY FOR DEFAULT USERS TABLE FOR
  // NOW Or if we had a way to know if table has index. For now, let's keep
  // index only on "users" table.
  if (strcmp(statement->table_name, "users") == 0) {
    // Hardcoded index root for users is 2
    // Ensure we have username
    char *username = statement->insert_values[1];
    if (username) {
      char username_buf[33]; // +1 for safety
      memset(username_buf, 0, 33);
      strncpy(username_buf, username, 32);

      Cursor *index_cursor = table_find(table, 2, username_buf, 32,
                                        KEY_STRING); // 32 is USERNAME_SIZE

      leaf_node_insert(index_cursor, username_buf, 32, &key_to_insert,
                       sizeof(uint32_t), KEY_STRING);
      free(index_cursor);
    }
  }

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table, int out_fd) {
  if (statement->has_join) {
    // JOIN Logic (Keep hardcoded for users/orders for now as per plan)
    // ... (Existing JOIN logic)
    // Actually, let's just leave JOIN as is, it uses table->main_root_page_num
    // which is gone. We need to fix JOIN to use find_table("users") and
    // find_table("orders").
    TableInfo *users_info = find_table(table, "users");
    TableInfo *orders_info = find_table(table, "orders");

    if (!users_info || !orders_info)
      return EXECUTE_SUCCESS;

    Cursor *user_cursor = table_start(table, users_info->root_page_num);

    while (!user_cursor->end_of_table) {
      Row user_row;
      deserialize_row(cursor_value(user_cursor), &user_row);

      Cursor *order_cursor = table_start(table, orders_info->root_page_num);
      while (!order_cursor->end_of_table) {
        OrderRow order_row;
        deserialize_order_row(cursor_value(order_cursor), &order_row);

        if (user_row.id == order_row.user_id) {
          dprintf(out_fd, "(%d, %s, %s) | (%d, %d, %s)\n", user_row.id,
                  user_row.username, user_row.email, order_row.id,
                  order_row.user_id, order_row.product_name);
        }

        cursor_advance(order_cursor);
      }
      free(order_cursor);

      cursor_advance(user_cursor);
    }
    free(user_cursor);
    return EXECUTE_SUCCESS;
  }

  TableInfo *table_info = find_table(table, statement->table_name);
  if (table_info == NULL) {
    dprintf(out_fd, "Error: Table '%s' not found.\n", statement->table_name);
    return EXECUTE_TABLE_FULL;
  }

  // Normal SELECT (Dynamic)
  Cursor *cursor = table_start(table, table_info->root_page_num);
  int rows_printed = 0;

  while (!cursor->end_of_table) {
    if (statement->limit != -1 && rows_printed >= statement->limit) {
      break;
    }

    void *node = get_page(table->pager, cursor->page_num);

    uint32_t row_size = 0;
    for (uint32_t i = 0; i < table_info->num_columns; i++)
      row_size += table_info->columns[i].size;

    void *row_data = leaf_node_value(node, cursor->cell_num, row_size + 4,
                                     4); // +4 for key size (INT)

    // Check WHERE condition
    int match = 1;
    if (statement->has_where) {
      match = 0;
      for (uint32_t i = 0; i < table_info->num_columns; i++) {
        Column *col = &table_info->columns[i];
        if (strcmp(col->name, statement->where_column) == 0) {
          void *val_ptr = row_data + col->offset;
          if (col->type == COLUMN_INT) {
            uint32_t val;
            memcpy(&val, val_ptr, sizeof(uint32_t));
            int where_val = atoi(statement->where_value);
            // Only support = for now
            if (val == (uint32_t)where_val)
              match = 1;
          } else {
            char *val = (char *)val_ptr;
            // Remove quotes from where_value if present
            char clean_where_val[255];
            strcpy(clean_where_val, statement->where_value);
            if (clean_where_val[0] == '\'') {
              memmove(clean_where_val, clean_where_val + 1,
                      strlen(clean_where_val));
              clean_where_val[strlen(clean_where_val) - 1] = '\0';
            }
            if (strcmp(val, clean_where_val) == 0)
              match = 1;
          }
          break;
        }
      }
    }

    if (match) {
      dprintf(out_fd, "(");

      int num_cols_to_print = statement->num_select_columns > 0
                                  ? statement->num_select_columns
                                  : table_info->num_columns;

      for (int i = 0; i < num_cols_to_print; i++) {
        Column *col = NULL;
        if (statement->num_select_columns > 0) {
          // Find column by name
          for (uint32_t j = 0; j < table_info->num_columns; j++) {
            if (strcmp(table_info->columns[j].name,
                       statement->select_columns[i]) == 0) {
              col = &table_info->columns[j];
              break;
            }
          }
        } else {
          col = &table_info->columns[i];
        }

        if (col) {
          void *val_ptr = row_data + col->offset;
          if (col->type == COLUMN_INT) {
            uint32_t val;
            memcpy(&val, val_ptr, sizeof(uint32_t));
            dprintf(out_fd, "%d", val);
          } else {
            dprintf(out_fd, "%s", (char *)val_ptr);
          }
        } else {
          dprintf(out_fd, "NULL"); // Column not found
        }

        if (i < num_cols_to_print - 1)
          dprintf(out_fd, ", ");
      }
      dprintf(out_fd, ")\n");
      rows_printed++;
    }

    cursor_advance(cursor);
  }
  free(cursor);
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_delete(Statement *statement, Table *table, int out_fd) {
  (void)out_fd; // Unused
  TableInfo *table_info = find_table(table, statement->table_name);
  if (!table_info)
    return EXECUTE_SUCCESS;

  Cursor *cursor = table_start(table, table_info->root_page_num);

  // Optimization for ID lookup removed to handle duplicates and ensure
  // correctness if (statement->has_where && strcmp(statement->where_column,
  // "id") == 0 &&
  //     strcmp(statement->where_operator, "=") == 0) {
  //   uint32_t id_to_find = atoi(statement->where_value);
  //   free(cursor);
  //   cursor = table_find(table, table_info->root_page_num, &id_to_find,
  //                       sizeof(uint32_t), KEY_INT);
  // }

  while (!cursor->end_of_table) {
    Row row;
    deserialize_row(cursor_value(cursor), &row);

    int pass = 1;
    if (statement->has_where) {
      if (strcmp(statement->where_column, "id") == 0) {
        int val = atoi(statement->where_value);
        if (strcmp(statement->where_operator, "=") == 0) {
          if (row.id != (uint32_t)val)
            pass = 0;
        }
      } else if (strcmp(statement->where_column, "username") == 0) {
        if (strcmp(statement->where_operator, "=") == 0) {
          if (strcmp(row.username, statement->where_value) != 0)
            pass = 0;
        }
      }
    }

    if (pass) {
      // Calculate row size for dynamic tables
      uint32_t row_size = 0;
      for (uint32_t i = 0; i < table_info->num_columns; i++)
        row_size += table_info->columns[i].size;

      uint32_t key_to_delete = row.id; // Assuming 'id' is the key for deletion
      leaf_node_delete(cursor, &key_to_delete, sizeof(uint32_t), row_size,
                       KEY_INT);

      // Delete from index only if users table
      if (strcmp(statement->table_name, "users") == 0) {
        Cursor *index_cursor =
            table_find(table, 2, row.username, USERNAME_SIZE, KEY_STRING);
        leaf_node_delete(index_cursor, row.username, USERNAME_SIZE,
                         sizeof(uint32_t), KEY_STRING);
        free(index_cursor);
      }

      // if (statement->has_where && strcmp(statement->where_column, "id") == 0
      // &&
      //     strcmp(statement->where_operator, "=") == 0) {
      //   break;
      // }

      // If we deleted a row, the next row shifts into the current position.
      // So we don't need to advance the cursor unless we are at the end of the
      // node.
      void *node = get_page(table->pager, cursor->page_num);
      uint32_t num_cells = *leaf_node_num_cells(node);
      if (cursor->cell_num >= num_cells) {
        cursor_advance(cursor);
      }
    } else {
      cursor_advance(cursor);
    }
  }
  free(cursor);
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_insert_select(Statement *statement, Table *table,
                                    int out_fd) {
  TableInfo *source_info = find_table(table, statement->select_source_table);
  TableInfo *dest_info = find_table(table, statement->table_name);

  if (!source_info || !dest_info) {
    dprintf(out_fd, "Error: Table not found.\n");
    return EXECUTE_TABLE_FULL;
  }

  Cursor *cursor = table_start(table, source_info->root_page_num);
  while (!cursor->end_of_table) {
    Row row;
    deserialize_row(cursor_value(cursor), &row);

    int pass = 1;
    if (statement->select_has_where) {
      if (strcmp(statement->select_where_column, "username") == 0) {
        if (strcmp(statement->select_where_operator, "=") == 0) {
          if (strcmp(row.username, statement->select_where_value) != 0)
            pass = 0;
        }
      }
    }

    if (pass) {
      OrderRow order;
      order.id = row.id + 1000;
      order.user_id = row.id;
      strcpy(order.product_name, "AutoImport");

      Cursor *order_cursor = table_find(table, dest_info->root_page_num,
                                        &order.id, sizeof(uint32_t), KEY_INT);
      leaf_node_insert(order_cursor, &order.id, sizeof(uint32_t), &order,
                       sizeof(OrderRow), KEY_INT);
      free(order_cursor);

      dprintf(out_fd, "Inserted Order %d for User %d\n", order.id,
              order.user_id);
    }

    cursor_advance(cursor);
  }
  free(cursor);
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_begin(Statement *statement, Table *table, int out_fd) {
  (void)statement;
  if (table->in_transaction) {
    print_msg(out_fd, "Error: Already in a transaction\n");
    return EXECUTE_SUCCESS;
  }
  table->in_transaction = true;
  print_msg(out_fd, "Transaction started.\n");
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_commit(Statement *statement, Table *table, int out_fd) {
  (void)statement;
  if (!table->in_transaction) {
    print_msg(out_fd, "Error: Not in a transaction\n");
    return EXECUTE_SUCCESS;
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    if (table->pager->pages[i] != NULL) {
      pager_flush(table->pager, i, PAGE_SIZE);
    }
  }

  table->in_transaction = false;
  print_msg(out_fd, "Transaction committed.\n");
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_create_table(Statement *statement, Table *table,
                                   int out_fd) {
  if (table->num_tables >= MAX_TABLES) {
    dprintf(out_fd, "Error: Max tables reached.\n");
    return EXECUTE_TABLE_FULL;
  }

  if (find_table(table, statement->create_table_name) != NULL) {
    dprintf(out_fd, "Error: Table already exists.\n");
    return EXECUTE_DUPLICATE_KEY;
  }

  // Allocate new page
  printf("Debug: Allocating new page for table %s\n",
         statement->create_table_name);
  uint32_t root_page_num = get_unused_page_num(table->pager);
  printf("Debug: New root page num: %d\n", root_page_num);
  void *root_node = get_page(table->pager, root_page_num);
  initialize_leaf_node(root_node);
  set_node_root(root_node, true);
  pager_flush(table->pager, root_page_num, PAGE_SIZE);

  // Add to table list
  TableInfo *new_table = &table->tables[table->num_tables];
  strcpy(new_table->name, statement->create_table_name);
  new_table->root_page_num = root_page_num;
  new_table->num_columns = statement->create_num_columns;

  uint32_t offset = 0;
  for (uint32_t i = 0; i < new_table->num_columns; i++) {
    strcpy(new_table->columns[i].name, statement->create_column_names[i]);
    if (statement->create_column_types[i] == 0) {
      new_table->columns[i].type = COLUMN_INT;
      new_table->columns[i].size = 4;
    } else {
      new_table->columns[i].type = COLUMN_VARCHAR;
      new_table->columns[i].size = 255; // Default size for now
    }
    new_table->columns[i].offset = offset;
    offset += new_table->columns[i].size;
  }

  table->num_tables++;

  printf("Debug: Table created successfully\n");
  dprintf(out_fd, "Table created.\n");
  fsync(out_fd); // Ensure it's sent
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_rollback(Statement *statement, Table *table, int out_fd) {
  (void)statement;
  if (!table->in_transaction) {
    print_msg(out_fd, "Error: Not in a transaction\n");
    return EXECUTE_SUCCESS;
  }

  pager_rollback(table->pager);

  table->in_transaction = false;
  print_msg(out_fd, "Transaction rolled back.\n");
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_show_tables(Table *table, int out_fd) {
  for (uint32_t i = 0; i < table->num_tables; i++) {
    dprintf(out_fd, "%s\n", table->tables[i].name);
  }
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_desc_table(Statement *statement, Table *table,
                                 int out_fd) {
  TableInfo *table_info = find_table(table, statement->desc_table_name);
  if (table_info == NULL) {
    dprintf(out_fd, "Error: Table '%s' not found.\n",
            statement->desc_table_name);
    return EXECUTE_SUCCESS;
  }

  dprintf(out_fd, "Column | Type | Size\n");
  dprintf(out_fd, "-------|------|-----\n");
  for (uint32_t i = 0; i < table_info->num_columns; i++) {
    Column *col = &table_info->columns[i];
    dprintf(out_fd, "%s | %s | %d\n", col->name,
            col->type == COLUMN_INT ? "INT" : "VARCHAR", col->size);
  }
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_show_index(Statement *statement, Table *table,
                                 int out_fd) {
  TableInfo *table_info = find_table(table, statement->desc_table_name);
  if (table_info == NULL) {
    dprintf(out_fd, "Error: Table '%s' not found.\n",
            statement->desc_table_name);
    return EXECUTE_SUCCESS;
  }

  dprintf(out_fd, "Table | Key Name | Column Name | Type\n");
  dprintf(out_fd, "------|----------|-------------|-----\n");

  // Primary Key (Always on first column for now)
  if (table_info->num_columns > 0) {
    dprintf(out_fd, "%s | PRIMARY | %s | CLUSTERED\n", table_info->name,
            table_info->columns[0].name);
  }

  // Hardcoded Secondary Index for users
  if (strcmp(table_info->name, "users") == 0) {
    dprintf(out_fd, "%s | username_idx | username | SECONDARY\n",
            table_info->name);
  }

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table,
                                int out_fd) {
  switch (statement->type) {
  case STATEMENT_INSERT:
    // Check table type to decide how to insert
    {
      return execute_insert(statement, table, out_fd);
    }
  case STATEMENT_SELECT:
    return execute_select(statement, table, out_fd);
  case STATEMENT_DELETE:
    return execute_delete(statement, table, out_fd);
  case STATEMENT_INSERT_SELECT:
    return execute_insert_select(statement, table, out_fd);
  case STATEMENT_BEGIN:
    return execute_begin(statement, table, out_fd);
  case STATEMENT_COMMIT:
    return execute_commit(statement, table, out_fd);
  case STATEMENT_ROLLBACK:
    return execute_rollback(statement, table, out_fd);
  case STATEMENT_CREATE_TABLE:
    return execute_create_table(statement, table, out_fd);
  case STATEMENT_SHOW_TABLES:
    return execute_show_tables(table, out_fd);
  case STATEMENT_DESC_TABLE:
    return execute_desc_table(statement, table, out_fd);
  case STATEMENT_SHOW_INDEX:
    return execute_show_index(statement, table, out_fd);
  default:
    return EXECUTE_SUCCESS;
  }
}
