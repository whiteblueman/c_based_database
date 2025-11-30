#include "node.h"
#include "cursor.h"
#include "table.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

NodeType get_node_type(void *node) {
  uint8_t value = *((uint8_t *)((char *)node + NODE_TYPE_OFFSET));
  return (NodeType)value;
}

void set_node_type(void *node, NodeType type) {
  uint8_t value = type;
  *((uint8_t *)((char *)node + NODE_TYPE_OFFSET)) = value;
}

bool is_node_root(void *node) {
  uint8_t value = *((uint8_t *)((char *)node + IS_ROOT_OFFSET));
  return (bool)value;
}

void set_node_root(void *node, bool is_root) {
  uint8_t value = is_root;
  *((uint8_t *)((char *)node + IS_ROOT_OFFSET)) = value;
}

uint32_t *node_parent(void *node) {
  return (uint32_t *)((char *)node + PARENT_POINTER_OFFSET);
}

void initialize_internal_node(void *node) {
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;
}

uint32_t *internal_node_num_keys(void *node) {
  return (uint32_t *)((char *)node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t *internal_node_right_child(void *node) {
  return (uint32_t *)((char *)node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

void *internal_node_cell(void *node, uint32_t cell_num, uint32_t cell_size) {
  return (char *)node + INTERNAL_NODE_HEADER_SIZE + cell_num * cell_size;
}

void *internal_node_key(void *node, uint32_t cell_num, uint32_t key_size,
                        uint32_t child_size) {
  return (char *)internal_node_cell(node, cell_num, key_size + child_size) +
         child_size;
}

uint32_t *internal_node_child(void *node, uint32_t cell_num, uint32_t key_size,
                              uint32_t child_size) {
  return (uint32_t *)internal_node_cell(node, cell_num, key_size + child_size);
}

void initialize_leaf_node(void *node) {
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
  *leaf_node_next_leaf(node) = 0;
}

uint32_t *leaf_node_num_cells(void *node) {
  return (uint32_t *)((char *)node + LEAF_NODE_NUM_CELLS_OFFSET);
}

uint32_t *leaf_node_next_leaf(void *node) {
  return (uint32_t *)((char *)node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

void *leaf_node_cell(void *node, uint32_t cell_num, uint32_t cell_size) {
  return (char *)node + LEAF_NODE_HEADER_SIZE + cell_num * cell_size;
}

void *leaf_node_key(void *node, uint32_t cell_num, uint32_t cell_size) {
  return leaf_node_cell(node, cell_num, cell_size);
}

void *leaf_node_value(void *node, uint32_t cell_num, uint32_t cell_size,
                      uint32_t key_size) {
  return (char *)leaf_node_cell(node, cell_num, cell_size) +
         LEAF_NODE_KEY_OFFSET + key_size;
}

int compare_keys(void *k1, void *k2, KeyType type, uint32_t key_size) {
  if (type == KEY_INT) {
    uint32_t v1 = *(uint32_t *)k1;
    uint32_t v2 = *(uint32_t *)k2;
    if (v1 < v2)
      return -1;
    if (v1 > v2)
      return 1;
    return 0;
  } else {
    // String key
    return strncmp((char *)k1, (char *)k2, key_size);
  }
}

// Helper to get pointer to max key
void *get_node_max_key_ptr(void *node, uint32_t key_size, uint32_t child_size,
                           uint32_t leaf_cell_size) {
  switch (get_node_type(node)) {
  case NODE_INTERNAL:
    return internal_node_key(node, *internal_node_num_keys(node) - 1, key_size,
                             child_size);
  case NODE_LEAF:
    return leaf_node_key(node, *leaf_node_num_cells(node) - 1, leaf_cell_size);
  }
  return NULL;
}

void create_new_root(Table *table, uint32_t root_page_num,
                     uint32_t right_child_page_num, uint32_t key_size,
                     uint32_t child_size, uint32_t leaf_cell_size) {
  void *root = get_page(table->pager, root_page_num);
  void *right_child = get_page(table->pager, right_child_page_num);
  uint32_t left_child_page_num = get_unused_page_num(table->pager);
  void *left_child = get_page(table->pager, left_child_page_num);

  if (get_node_type(root) == NODE_INTERNAL) {
    initialize_internal_node(right_child);
    initialize_internal_node(left_child);
  }

  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, false);

  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1;
  *internal_node_right_child(root) = right_child_page_num;

  void *left_max_key =
      get_node_max_key_ptr(left_child, key_size, child_size, leaf_cell_size);
  memcpy(internal_node_key(root, 0, key_size, child_size), left_max_key,
         key_size);

  *internal_node_child(root, 0, key_size, child_size) = left_child_page_num;
  *node_parent(left_child) = root_page_num;
  *node_parent(right_child) = root_page_num;
}

void leaf_node_split_and_insert(Cursor *cursor, void *key, uint32_t key_size,
                                void *value, uint32_t value_size,
                                KeyType key_type) {
  void *old_node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t cell_size = key_size + value_size;
  uint32_t max_cells = (PAGE_SIZE - LEAF_NODE_HEADER_SIZE) / cell_size;

  void *old_max_key =
      leaf_node_key(old_node, *leaf_node_num_cells(old_node) - 1, cell_size);

  uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
  void *new_node = get_page(cursor->table->pager, new_page_num);
  initialize_leaf_node(new_node);
  set_node_root(new_node, false);
  *node_parent(new_node) = *node_parent(old_node);

  for (int32_t i = max_cells; i >= 0; i--) {
    void *destination_node;
    uint32_t index_within_node;
    if ((uint32_t)i >= max_cells / 2) {
      destination_node = new_node;
      index_within_node = i - (max_cells / 2);
    } else {
      destination_node = old_node;
      index_within_node = i;
    }
    void *destination =
        leaf_node_cell(destination_node, index_within_node, cell_size);

    if ((uint32_t)i == cursor->cell_num) {
      memcpy(leaf_node_value(destination_node, index_within_node, cell_size,
                             key_size),
             value, value_size);
      memcpy(leaf_node_key(destination_node, index_within_node, cell_size), key,
             key_size);
    } else if ((uint32_t)i > cursor->cell_num) {
      memcpy(destination, leaf_node_cell(old_node, i - 1, cell_size),
             cell_size);
    } else {
      memcpy(destination, leaf_node_cell(old_node, i, cell_size), cell_size);
    }
  }

  *(leaf_node_num_cells(old_node)) = max_cells / 2;
  *(leaf_node_num_cells(new_node)) = max_cells + 1 - (max_cells / 2);
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  *leaf_node_next_leaf(old_node) = new_page_num;

  if (is_node_root(old_node)) {
    create_new_root(cursor->table, cursor->page_num, new_page_num, key_size,
                    sizeof(uint32_t), cell_size);
  } else {
    uint32_t parent_page_num = *node_parent(old_node);
    void *new_max_key =
        leaf_node_key(old_node, *leaf_node_num_cells(old_node) - 1, cell_size);

    void *parent = get_page(cursor->table->pager, parent_page_num);

    uint32_t index = internal_node_find_child(parent, old_max_key, key_size,
                                              sizeof(uint32_t), key_type);
    uint32_t num_keys = *internal_node_num_keys(parent);

    if (index == num_keys) {
      /* Splitting right child */
      *internal_node_child(parent, index, key_size, sizeof(uint32_t)) =
          cursor->page_num;
      memcpy(internal_node_key(parent, index, key_size, sizeof(uint32_t)),
             new_max_key, key_size);
      *internal_node_right_child(parent) = new_page_num;
      *internal_node_num_keys(parent) = num_keys + 1;
    } else {
      /* Splitting regular child */
      memcpy(internal_node_key(parent, index, key_size, sizeof(uint32_t)),
             new_max_key, key_size);
      internal_node_insert(cursor->table, parent_page_num, new_page_num,
                           key_size, sizeof(uint32_t), key_type);
    }
    return;
  }
}

Cursor *leaf_node_find(Table *table, uint32_t page_num, void *key,
                       uint32_t key_size, uint32_t value_size,
                       KeyType key_type) {
  void *node = get_page(table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = page_num;

  uint32_t cell_size = key_size + value_size;

  // Binary search
  uint32_t min_index = 0;
  uint32_t max_index = num_cells;
  while (min_index != max_index) {
    uint32_t index = (min_index + max_index) / 2;
    void *key_at_index = leaf_node_key(node, index, cell_size);
    int cmp = compare_keys(key, key_at_index, key_type, key_size);
    if (cmp == 0) {
      cursor->cell_num = index;
      return cursor;
    }
    if (cmp < 0) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  cursor->cell_num = min_index;
  return cursor;
}

void leaf_node_insert(Cursor *cursor, void *key, uint32_t key_size, void *value,
                      uint32_t value_size, KeyType key_type) {
  void *node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t num_cells = *leaf_node_num_cells(node);
  uint32_t cell_size = key_size + value_size;
  uint32_t max_cells = (PAGE_SIZE - LEAF_NODE_HEADER_SIZE) / cell_size;

  if (num_cells >= max_cells) {
    leaf_node_split_and_insert(cursor, key, key_size, value, value_size,
                               key_type);
    return;
  }

  if (cursor->cell_num < num_cells) {
    // Make room for new cell
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i, cell_size),
             leaf_node_cell(node, i - 1, cell_size), cell_size);
    }
  }

  *(leaf_node_num_cells(node)) += 1;
  memcpy(leaf_node_key(node, cursor->cell_num, cell_size), key, key_size);
  memcpy(leaf_node_value(node, cursor->cell_num, cell_size, key_size), value,
         value_size);
}

void leaf_node_delete(Cursor *cursor, void *key, uint32_t key_size,
                      KeyType key_type) {
  void *node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (cursor->cell_num >= num_cells) {
    return; // Cell not found or invalid cursor
  }

  uint32_t value_size;
  if (key_type == KEY_INT) {
    value_size = MAIN_TABLE_VALUE_SIZE;
  } else {
    value_size = USERNAME_INDEX_VALUE_SIZE;
  }
  uint32_t cell_size = key_size + value_size;

  void *key_at_index = leaf_node_key(node, cursor->cell_num, cell_size);
  if (compare_keys(key_at_index, key, key_type, key_size) != 0) {
    return; // Key mismatch
  }

  // Shift cells left
  for (uint32_t i = cursor->cell_num; i < num_cells - 1; i++) {
    memcpy(leaf_node_cell(node, i, cell_size),
           leaf_node_cell(node, i + 1, cell_size), cell_size);
  }

  *(leaf_node_num_cells(node)) -= 1;
}

void internal_node_insert(Table *table, uint32_t parent_page_num,
                          uint32_t child_page_num, uint32_t key_size,
                          uint32_t child_size, KeyType key_type) {
  void *parent = get_page(table->pager, parent_page_num);
  void *child = get_page(table->pager, child_page_num);

  uint32_t leaf_cell_size;
  if (key_type == KEY_INT) {
    leaf_cell_size = MAIN_TABLE_LEAF_CELL_SIZE;
  } else {
    leaf_cell_size = USERNAME_INDEX_LEAF_CELL_SIZE;
  }

  void *child_max_key =
      get_node_max_key_ptr(child, key_size, child_size, leaf_cell_size);
  uint32_t num_keys = *internal_node_num_keys(parent);

  uint32_t cell_size = key_size + child_size;
  uint32_t max_cells = (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE) / cell_size;

  if (num_keys >= max_cells) {
    internal_node_split_and_insert(table, parent_page_num, child_page_num,
                                   key_size, child_size, key_type);
    return;
  }

  uint32_t index = internal_node_find_child(parent, child_max_key, key_size,
                                            child_size, key_type);

  for (uint32_t i = num_keys; i > index; i--) {
    memcpy(internal_node_cell(parent, i, cell_size),
           internal_node_cell(parent, i - 1, cell_size), cell_size);
  }

  *internal_node_num_keys(parent) = num_keys + 1;
  *internal_node_child(parent, index, key_size, child_size) = child_page_num;
  memcpy(internal_node_key(parent, index, key_size, child_size), child_max_key,
         key_size);
}

void internal_node_split_and_insert(Table *table, uint32_t parent_page_num,
                                    uint32_t child_page_num, uint32_t key_size,
                                    uint32_t child_size, KeyType key_type) {
  uint32_t old_page_num = parent_page_num;
  void *old_node = get_page(table->pager, parent_page_num);

  uint32_t leaf_cell_size;
  if (key_type == KEY_INT) {
    leaf_cell_size = MAIN_TABLE_LEAF_CELL_SIZE;
  } else {
    leaf_cell_size = USERNAME_INDEX_LEAF_CELL_SIZE;
  }

  void *old_max =
      get_node_max_key_ptr(old_node, key_size, child_size, leaf_cell_size);

  void *child = get_page(table->pager, child_page_num);
  void *child_max =
      get_node_max_key_ptr(child, key_size, child_size, leaf_cell_size);

  uint32_t new_page_num = get_unused_page_num(table->pager);
  uint32_t splitting_root = is_node_root(old_node);

  void *parent;
  void *new_node;
  if (splitting_root) {
    create_new_root(table, parent_page_num, new_page_num, key_size, child_size,
                    leaf_cell_size);
    parent = get_page(table->pager, parent_page_num);
    old_page_num = *internal_node_child(parent, 0, key_size, child_size);
    old_node = get_page(table->pager, old_page_num);
  } else {
    parent = get_page(table->pager, *node_parent(old_node));
    new_node = get_page(table->pager, new_page_num);
    initialize_internal_node(new_node);
  }

  uint32_t *old_num_keys = internal_node_num_keys(old_node);
  uint32_t cur_page_num = *internal_node_right_child(old_node);
  void *cur_node = get_page(table->pager, cur_page_num);

  internal_node_insert(table, new_page_num, cur_page_num, key_size, child_size,
                       key_type);
  *node_parent(cur_node) = new_page_num;
  *internal_node_right_child(old_node) = INVALID_PAGE_NUM;

  uint32_t cell_size = key_size + child_size;
  uint32_t max_cells = (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE) / cell_size;

  for (uint32_t i = max_cells - 1; i > max_cells / 2; i--) {
    cur_page_num = *internal_node_child(old_node, i, key_size, child_size);
    cur_node = get_page(table->pager, cur_page_num);

    internal_node_insert(table, new_page_num, cur_page_num, key_size,
                         child_size, key_type);
    *node_parent(cur_node) = new_page_num;

    (*old_num_keys)--;
  }

  *internal_node_right_child(old_node) =
      *internal_node_child(old_node, *old_num_keys - 1, key_size, child_size);
  (*old_num_keys)--;

  void *max_after_split =
      get_node_max_key_ptr(old_node, key_size, child_size, leaf_cell_size);

  uint32_t destination_page_num = old_page_num;
  if (compare_keys(child_max, max_after_split, key_type, key_size) > 0) {
    destination_page_num = new_page_num;
  }

  internal_node_insert(table, destination_page_num, child_page_num, key_size,
                       child_size, key_type);
  *node_parent(child) = destination_page_num;

  update_internal_node_key(
      parent, old_max,
      get_node_max_key_ptr(old_node, key_size, child_size, leaf_cell_size),
      key_size, child_size, key_type);

  if (!splitting_root) {
    internal_node_insert(table, *node_parent(old_node), new_page_num, key_size,
                         child_size, key_type);
    *node_parent(new_node) = *node_parent(old_node);
  }
}

void update_internal_node_key(void *node, void *old_key, void *new_key,
                              uint32_t key_size, uint32_t child_size,
                              KeyType key_type) {
  uint32_t old_child_index =
      internal_node_find_child(node, old_key, key_size, child_size, key_type);
  memcpy(internal_node_key(node, old_child_index, key_size, child_size),
         new_key, key_size);
}

uint32_t internal_node_find_child(void *node, void *key, uint32_t key_size,
                                  uint32_t child_size, KeyType key_type) {
  uint32_t num_keys = *internal_node_num_keys(node);

  /* Binary search */
  uint32_t min_index = 0;
  uint32_t max_index = num_keys;

  while (min_index != max_index) {
    uint32_t index = (min_index + max_index) / 2;
    void *key_to_right = internal_node_key(node, index, key_size, child_size);
    if (compare_keys(key_to_right, key, key_type, key_size) >= 0) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  return min_index;
}
