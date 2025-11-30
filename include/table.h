#ifndef TABLE_H
#define TABLE_H

#include "pager.h"
#include "row.h"
#include <stdint.h>

#define PAGE_SIZE 4096

typedef struct Table {
  uint32_t num_rows;
  Pager *pager;
  uint32_t main_root_page_num;
  uint32_t index_root_page_num;
  uint32_t orders_root_page_num;
} Table;

extern const uint32_t ROWS_PER_PAGE;

Table *db_open(const char *filename);
void db_close(Table *table);
void *row_slot(Table *table, uint32_t row_num);

void serialize_row(Row *source, void *destination);
void deserialize_row(void *source, Row *destination);

#endif
