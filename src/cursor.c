#include "cursor.h"
#include "node.h"
#include "table.h"
#include <stdio.h>
#include <stdlib.h>

Cursor *table_start(Table *table, uint32_t root_page_num) {
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;

  uint32_t page_num = root_page_num;
  void *node = get_page(table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  while (get_node_type(node) == NODE_INTERNAL) {
    uint32_t child_page_num = *internal_node_child(
        node, 0, MAIN_TABLE_INTERNAL_KEY_SIZE, MAIN_TABLE_INTERNAL_CHILD_SIZE);
    page_num = child_page_num;
    node = get_page(table->pager, page_num);
  }

  cursor->page_num = page_num;
  cursor->cell_num = 0;

  num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

Cursor *table_end(Table *table, uint32_t root_page_num) {
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = root_page_num;

  void *root_node = get_page(table->pager, root_page_num);

  while (get_node_type(root_node) == NODE_INTERNAL) {
    uint32_t right_child_page_num = *internal_node_right_child(root_node);
    root_node = get_page(table->pager, right_child_page_num);
    cursor->page_num = right_child_page_num;
  }

  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->cell_num = num_cells;
  cursor->end_of_table = true;

  return cursor;
}

Cursor *table_find(Table *table, uint32_t root_page_num, void *key,
                   uint32_t key_size, KeyType key_type) {
  void *root_node = get_page(table->pager, root_page_num);

  if (get_node_type(root_node) == NODE_LEAF) {
    uint32_t value_size = (key_type == KEY_INT) ? MAIN_TABLE_VALUE_SIZE
                                                : USERNAME_INDEX_VALUE_SIZE;
    return leaf_node_find(table, root_page_num, key, key_size, value_size,
                          key_type);
  } else {
    uint32_t child_size = sizeof(uint32_t);
    uint32_t num_keys = *internal_node_num_keys(root_node);
    uint32_t child_index = internal_node_find_child(root_node, key, key_size,
                                                    child_size, key_type);
    uint32_t child_page_num;
    if (child_index >= num_keys) {
      child_page_num = *internal_node_right_child(root_node);
    } else {
      child_page_num =
          *internal_node_child(root_node, child_index, key_size, child_size);
    }

    return table_find(table, child_page_num, key, key_size, key_type);
  }
}

void *cursor_value(Cursor *cursor) {
  uint32_t page_num = cursor->page_num;
  void *page = get_page(cursor->table->pager, page_num);

  // We need to know if we are in Main Table or Index to know value size/offset?
  // cursor_value is mainly used for Main Table (SELECT *).
  // For Index, we extract value manually in execute_select.
  // So let's assume Main Table for now.
  // Or we can check if page belongs to Main or Index? No easy way.
  // But cursor_value returns void*.
  // leaf_node_value needs key_size.
  // Main Table key size is 4.
  // Index key size is 32.
  // We don't know which one it is here.
  // But cursor_value is called by deserialize_row which expects Main Table Row.
  // So it's safe to assume Main Table for now.

  return leaf_node_value(page, cursor->cell_num, MAIN_TABLE_LEAF_CELL_SIZE,
                         MAIN_TABLE_KEY_SIZE);
}

void cursor_advance(Cursor *cursor) {
  uint32_t page_num = cursor->page_num;
  void *node = get_page(cursor->table->pager, page_num);

  cursor->cell_num += 1;
  if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
    /* Advance to next leaf node */
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num == 0) {
      /* This was rightmost leaf */
      cursor->end_of_table = true;
    } else {
      cursor->page_num = next_page_num;
      cursor->cell_num = 0;
    }
  }
}
