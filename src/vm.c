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
  void *node = get_page(table->pager, table->main_root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  Row *row_to_insert = &(statement->row_to_insert);
  uint32_t key_to_insert = row_to_insert->id;
  Cursor *cursor = table_find(table, table->main_root_page_num, &key_to_insert,
                              sizeof(uint32_t), KEY_INT);

  if (cursor->cell_num < num_cells) {
    void *page = get_page(table->pager, cursor->page_num);
    uint32_t key_at_index = *(uint32_t *)leaf_node_key(
        page, cursor->cell_num, MAIN_TABLE_LEAF_CELL_SIZE);
    if (key_at_index == key_to_insert) {
      print_msg(out_fd, "Error: Duplicate key.\n");
      free(cursor);
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  // Insert into Main Table
  leaf_node_insert(cursor, &row_to_insert->id, sizeof(uint32_t), row_to_insert,
                   sizeof(Row), KEY_INT);
  free(cursor);

  // Insert into Secondary Index (Username)
  Cursor *index_cursor =
      table_find(table, table->index_root_page_num, row_to_insert->username,
                 USERNAME_SIZE, KEY_STRING);

  leaf_node_insert(index_cursor, row_to_insert->username, USERNAME_SIZE,
                   &row_to_insert->id, sizeof(uint32_t), KEY_STRING);
  free(index_cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table, int out_fd) {
  if (statement->has_join) {
    // JOIN Logic
    Cursor *user_cursor = table_start(table, table->main_root_page_num);

    while (!user_cursor->end_of_table) {
      Row user_row;
      deserialize_row(cursor_value(user_cursor), &user_row);

      Cursor *order_cursor = table_start(table, table->orders_root_page_num);
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

  // Normal SELECT
  Cursor *cursor = table_start(table, table->main_root_page_num);

  // Optimization: ID Index Scan
  if (statement->has_where && strcmp(statement->where_column, "id") == 0 &&
      strcmp(statement->where_operator, "=") == 0) {
    uint32_t id_to_find = atoi(statement->where_value);
    free(cursor);
    cursor = table_find(table, table->main_root_page_num, &id_to_find,
                        sizeof(uint32_t), KEY_INT);
  }
  // Optimization: Username Index Scan
  else if (statement->has_where &&
           strcmp(statement->where_column, "username") == 0 &&
           strcmp(statement->where_operator, "=") == 0) {
    char *username_to_find = statement->where_value;
    free(cursor);
    Cursor *index_cursor =
        table_find(table, table->index_root_page_num, username_to_find,
                   USERNAME_SIZE, KEY_STRING);

    if (!index_cursor->end_of_table) {
      void *page = get_page(table->pager, index_cursor->page_num);
      char *key_at_index = (char *)leaf_node_key(page, index_cursor->cell_num,
                                                 USERNAME_INDEX_LEAF_CELL_SIZE);
      if (strncmp(key_at_index, username_to_find, USERNAME_SIZE) == 0) {
        uint32_t *id_ptr = (uint32_t *)leaf_node_value(
            page, index_cursor->cell_num, USERNAME_INDEX_LEAF_CELL_SIZE,
            USERNAME_INDEX_KEY_SIZE);
        uint32_t id = *id_ptr;
        cursor = table_find(table, table->main_root_page_num, &id,
                            sizeof(uint32_t), KEY_INT);
      } else {
        cursor = table_end(table, table->main_root_page_num);
      }
    } else {
      cursor = table_end(table, table->main_root_page_num);
    }
    free(index_cursor);
  }

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
      } else if (strcmp(statement->where_column, "email") == 0) {
        if (strcmp(statement->where_operator, "=") == 0) {
          if (strcmp(row.email, statement->where_value) != 0)
            pass = 0;
        }
      }
    }

    if (pass) {
      print_row(&row, out_fd);
    }

    if (statement->has_where && strcmp(statement->where_column, "id") == 0 &&
        strcmp(statement->where_operator, "=") == 0) {
      break;
    }

    cursor_advance(cursor);
  }

  free(cursor);
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_delete(Statement *statement, Table *table, int out_fd) {
  (void)out_fd; // Unused
  Cursor *cursor = table_start(table, table->main_root_page_num);

  if (statement->has_where && strcmp(statement->where_column, "id") == 0 &&
      strcmp(statement->where_operator, "=") == 0) {
    uint32_t id_to_find = atoi(statement->where_value);
    free(cursor);
    cursor = table_find(table, table->main_root_page_num, &id_to_find,
                        sizeof(uint32_t), KEY_INT);
  }

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
      leaf_node_delete(cursor, &row.id, sizeof(uint32_t), KEY_INT);

      Cursor *index_cursor =
          table_find(table, table->index_root_page_num, row.username,
                     USERNAME_SIZE, KEY_STRING);
      leaf_node_delete(index_cursor, row.username, USERNAME_SIZE, KEY_STRING);
      free(index_cursor);

      if (statement->has_where && strcmp(statement->where_column, "id") == 0 &&
          strcmp(statement->where_operator, "=") == 0) {
        break;
      }
      break;
    }
    cursor_advance(cursor);
  }
  free(cursor);
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_insert_select(Statement *statement, Table *table,
                                    int out_fd) {
  Cursor *cursor = table_start(table, table->main_root_page_num);
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

      Cursor *order_cursor = table_find(table, table->orders_root_page_num,
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

ExecuteResult execute_statement(Statement *statement, Table *table,
                                int out_fd) {
  switch (statement->type) {
  case STATEMENT_INSERT:
    if (strcmp(statement->table_name, "orders") == 0) {
      Cursor *cursor = table_find(table, table->orders_root_page_num,
                                  &(statement->order_to_insert.id),
                                  sizeof(uint32_t), KEY_INT);
      leaf_node_insert(cursor, &(statement->order_to_insert.id),
                       sizeof(uint32_t), &(statement->order_to_insert),
                       sizeof(OrderRow), KEY_INT);
      free(cursor);
      return EXECUTE_SUCCESS;
    } else {
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
  }
  return EXECUTE_SUCCESS;
}
