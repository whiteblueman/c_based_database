#ifndef TABLE_H
#define TABLE_H

#include "pager.h"
#include "row.h"
#include <stdbool.h>
#include <stdint.h>

#define PAGE_SIZE 4096

#define MAX_TABLES 10
#define TABLE_NAME_SIZE 32

#define MAX_COLUMNS 10

typedef enum { COLUMN_INT, COLUMN_VARCHAR } ColumnType;

typedef struct {
  char name[32];
  ColumnType type;
  uint32_t size;
  uint32_t offset;
} Column;

typedef struct {
  char name[TABLE_NAME_SIZE];
  uint32_t root_page_num;
  uint32_t num_columns;
  Column columns[MAX_COLUMNS];
} TableInfo;

typedef struct Table {
  uint32_t num_rows; // This might need to be per-table or removed if unused
  Pager *pager;

  // Dynamic Table Directory
  uint32_t directory_root_page_num;
  uint32_t num_tables;
  TableInfo tables[MAX_TABLES];

  bool in_transaction;
} Table;

extern const uint32_t ROWS_PER_PAGE;

Table *db_open(const char *filename);
void db_close(Table *table);
TableInfo *find_table(Table *table, const char *name);
void *row_slot(Table *table, uint32_t row_num);

void serialize_row(Row *source, void *destination);
void deserialize_row(void *source, Row *destination);

#endif
