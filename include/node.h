#ifndef NODE_H
#define NODE_H

#include "row.h"
#include "table.h"
#include <stdbool.h>
#include <stdint.h>

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;
typedef enum { KEY_INT, KEY_STRING } KeyType;

/*
 * Common Node Header Layout
 */
#define NODE_TYPE_SIZE sizeof(uint8_t)
#define NODE_TYPE_OFFSET 0
#define IS_ROOT_SIZE sizeof(uint8_t)
#define IS_ROOT_OFFSET (NODE_TYPE_SIZE)
#define PARENT_POINTER_SIZE sizeof(uint32_t)
#define PARENT_POINTER_OFFSET (IS_ROOT_OFFSET + IS_ROOT_SIZE)
#define COMMON_NODE_HEADER_SIZE                                                \
  (NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE)

/*
 * Internal Node Header Layout
 */
#define INTERNAL_NODE_NUM_KEYS_SIZE sizeof(uint32_t)
#define INTERNAL_NODE_NUM_KEYS_OFFSET COMMON_NODE_HEADER_SIZE
#define INTERNAL_NODE_RIGHT_CHILD_SIZE sizeof(uint32_t)
#define INTERNAL_NODE_RIGHT_CHILD_OFFSET                                       \
  (INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE)
#define INTERNAL_NODE_HEADER_SIZE                                              \
  (COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE +                     \
   INTERNAL_NODE_RIGHT_CHILD_SIZE)

/*
 * Internal Node Body Layout
 */
#define INTERNAL_NODE_CHILD_SIZE sizeof(uint32_t)
// INTERNAL_NODE_KEY_SIZE is variable
// INTERNAL_NODE_CELL_SIZE is variable
#define INTERNAL_NODE_SPACE_FOR_CELLS (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE)

/*
 * Leaf Node Header Layout
 */
#define LEAF_NODE_NUM_CELLS_SIZE sizeof(uint32_t)
#define LEAF_NODE_NUM_CELLS_OFFSET COMMON_NODE_HEADER_SIZE
#define LEAF_NODE_NEXT_LEAF_SIZE sizeof(uint32_t)
#define LEAF_NODE_NEXT_LEAF_OFFSET                                             \
  (LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE)
#define LEAF_NODE_HEADER_SIZE                                                  \
  (COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE +                        \
   LEAF_NODE_NEXT_LEAF_SIZE)

/*
 * Leaf Node Body Layout
 */
// LEAF_NODE_KEY_SIZE is variable
#define LEAF_NODE_KEY_OFFSET 0
// LEAF_NODE_VALUE_SIZE is variable
// LEAF_NODE_CELL_SIZE is variable
#define LEAF_NODE_SPACE_FOR_CELLS (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)

void initialize_internal_node(void *node);
uint32_t *internal_node_num_keys(void *node);
uint32_t *internal_node_right_child(void *node);
void *internal_node_cell(void *node, uint32_t cell_num, uint32_t cell_size);
void *internal_node_key(void *node, uint32_t cell_num, uint32_t key_size,
                        uint32_t child_size);
uint32_t *internal_node_child(void *node, uint32_t cell_num, uint32_t key_size,
                              uint32_t child_size);
uint32_t get_node_max_key(
    void *node); // Needs update to handle generic keys? For now assume int keys
                 // for max_key logic or refactor later.
// Actually get_node_max_key is specific to INT keys. We might need
// get_node_max_key_int.

NodeType get_node_type(void *node);
void set_node_type(void *node, NodeType type);
bool is_node_root(void *node);
void set_node_root(void *node, bool is_root);
uint32_t *node_parent(void *node);

void initialize_leaf_node(void *node);
uint32_t *leaf_node_num_cells(void *node);
void *leaf_node_cell(void *node, uint32_t cell_num, uint32_t cell_size);
uint32_t *leaf_node_next_leaf(void *node);
void *leaf_node_key(void *node, uint32_t cell_num, uint32_t cell_size);
void *leaf_node_value(void *node, uint32_t cell_num, uint32_t cell_size,
                      uint32_t key_size);

typedef struct Cursor Cursor;
void leaf_node_insert(Cursor *cursor, void *key, uint32_t key_size, void *value,
                      uint32_t value_size, KeyType key_type);
Cursor *leaf_node_find(Table *table, uint32_t page_num, void *key,
                       uint32_t key_size, uint32_t value_size,
                       KeyType key_type);
void leaf_node_delete(Cursor *cursor, void *key, uint32_t key_size,
                      KeyType key_type);

void internal_node_insert(Table *table, uint32_t parent_page_num,
                          uint32_t child_page_num, uint32_t key_size,
                          uint32_t child_size, KeyType key_type);
void internal_node_split_and_insert(Table *table, uint32_t parent_page_num,
                                    uint32_t child_page_num, uint32_t key_size,
                                    uint32_t child_size, KeyType key_type);
void update_internal_node_key(void *node, void *old_key, void *new_key,
                              uint32_t key_size, uint32_t child_size,
                              KeyType key_type);
uint32_t internal_node_find_child(void *node, void *key, uint32_t key_size,
                                  uint32_t child_size, KeyType key_type);
void create_new_root(Table *table, uint32_t root_page_num,
                     uint32_t right_child_page_num, uint32_t key_size,
                     uint32_t child_size, uint32_t leaf_cell_size);

int compare_keys(void *k1, void *k2, KeyType type, uint32_t key_size);

#define MAIN_TABLE_KEY_SIZE sizeof(uint32_t)
#define MAIN_TABLE_VALUE_SIZE ROW_SIZE
#define MAIN_TABLE_LEAF_CELL_SIZE (MAIN_TABLE_KEY_SIZE + MAIN_TABLE_VALUE_SIZE)
#define MAIN_TABLE_LEAF_SPACE_FOR_CELLS (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)
#define MAIN_TABLE_LEAF_MAX_CELLS                                              \
  (MAIN_TABLE_LEAF_SPACE_FOR_CELLS / MAIN_TABLE_LEAF_CELL_SIZE)

#define MAIN_TABLE_INTERNAL_KEY_SIZE sizeof(uint32_t)
#define MAIN_TABLE_INTERNAL_CHILD_SIZE sizeof(uint32_t)
#define MAIN_TABLE_INTERNAL_CELL_SIZE                                          \
  (MAIN_TABLE_INTERNAL_KEY_SIZE + MAIN_TABLE_INTERNAL_CHILD_SIZE)
#define MAIN_TABLE_INTERNAL_SPACE_FOR_CELLS                                    \
  (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE)
#define MAIN_TABLE_INTERNAL_MAX_CELLS                                          \
  (MAIN_TABLE_INTERNAL_SPACE_FOR_CELLS / MAIN_TABLE_INTERNAL_CELL_SIZE)

#define USERNAME_INDEX_KEY_SIZE 32
#define USERNAME_INDEX_VALUE_SIZE sizeof(uint32_t)
#define USERNAME_INDEX_LEAF_CELL_SIZE                                          \
  (USERNAME_INDEX_KEY_SIZE + USERNAME_INDEX_VALUE_SIZE)
#define USERNAME_INDEX_LEAF_SPACE_FOR_CELLS (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)
#define USERNAME_INDEX_LEAF_MAX_CELLS                                          \
  (USERNAME_INDEX_LEAF_SPACE_FOR_CELLS / USERNAME_INDEX_LEAF_CELL_SIZE)

#define USERNAME_INDEX_INTERNAL_KEY_SIZE 32
#define USERNAME_INDEX_INTERNAL_CHILD_SIZE sizeof(uint32_t)
#define USERNAME_INDEX_INTERNAL_CELL_SIZE                                      \
  (USERNAME_INDEX_INTERNAL_KEY_SIZE + USERNAME_INDEX_INTERNAL_CHILD_SIZE)
#define USERNAME_INDEX_INTERNAL_SPACE_FOR_CELLS                                \
  (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE)
#define USERNAME_INDEX_INTERNAL_MAX_CELLS                                      \
  (USERNAME_INDEX_INTERNAL_SPACE_FOR_CELLS / USERNAME_INDEX_INTERNAL_CELL_SIZE)

#define INVALID_PAGE_NUM UINT32_MAX

#endif
