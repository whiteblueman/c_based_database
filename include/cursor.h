#ifndef CURSOR_H
#define CURSOR_H

#include "node.h"
#include "table.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct Cursor {
  Table *table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;
} Cursor;

Cursor *table_start(Table *table, uint32_t root_page_num);
Cursor *table_end(Table *table, uint32_t root_page_num);
Cursor *table_find(Table *table, uint32_t root_page_num, void *key,
                   uint32_t key_size, KeyType key_type);
void *cursor_value(Cursor *cursor);
void cursor_advance(Cursor *cursor);

#endif
