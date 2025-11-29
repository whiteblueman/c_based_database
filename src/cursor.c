#include "cursor.h"
#include "node.h"
#include <stdlib.h>

Cursor *table_start(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;

  uint32_t page_num = table->root_page_num;
  void *node = get_page(table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  // For now, assume root is leaf or handle basic traversal if internal
  // In a full implementation, we'd traverse down to the leftmost leaf
  // But since we only split root, the root might be internal.
  // If root is internal, we need to follow child 0 down.

  while (get_node_type(node) == NODE_INTERNAL) {
    page_num = *internal_node_child(node, 0);
    node = get_page(table->pager, page_num);
  }

  cursor->page_num = page_num;
  cursor->cell_num = 0;

  num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

Cursor *table_end(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num =
      table->root_page_num; // Not strictly correct but sufficient for now
  cursor->cell_num = 0;
  cursor->end_of_table = true;

  return cursor;
}

void *cursor_value(Cursor *cursor) {
  uint32_t page_num = cursor->page_num;
  void *page = get_page(cursor->table->pager, page_num);
  return leaf_node_value(page, cursor->cell_num);
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
