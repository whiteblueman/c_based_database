#ifndef COMPILER_H
#define COMPILER_H

#include "input_buffer.h"
#include "row.h"
#include <stdint.h>
#include <stdlib.h>

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_NEGATIVE_ID,
  PREPARE_STRING_TOO_LONG,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
  STATEMENT_DELETE,
  STATEMENT_INSERT_SELECT,
  STATEMENT_BEGIN,
  STATEMENT_COMMIT,
  STATEMENT_ROLLBACK,
  STATEMENT_CREATE_TABLE,
  STATEMENT_SHOW_TABLES,
  STATEMENT_DESC_TABLE,
  STATEMENT_SHOW_INDEX
} StatementType;

typedef struct {
  StatementType type;
  Row row_to_insert;
  OrderRow order_to_insert; // For inserting into orders
  char table_name[32];      // For INSERT/SELECT
  char where_column[32];
  char where_operator[3];
  char where_value[255];
  int has_where;
  int has_join;
  char join_table_name[32];
  char join_condition_left[32];  // e.g. users.id
  char join_condition_right[32]; // e.g. orders.user_id
  int limit;                     // -1 for no limit

  char select_columns[10][32];
  int num_select_columns; // 0 means *

  // For INSERT INTO ... SELECT ...
  char select_source_table[32];
  int select_has_where;
  char select_where_column[32];
  char select_where_operator[3];
  char select_where_value[255];

  // For CREATE TABLE
  char create_table_name[32];
  uint32_t create_num_columns;
  char create_column_names[10][32]; // MAX_COLUMNS = 10
  int create_column_types[10];      // 0 = INT, 1 = VARCHAR
  int create_schema_type;           // 0=User, 1=Order

  // For INSERT (Dynamic)
  char *insert_values[10]; // Pointers to tokens in input buffer

  // For DESC TABLE
  char desc_table_name[32];
} Statement;

typedef struct Table Table;

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table,
                                  int out_fd);
PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement);

#endif
