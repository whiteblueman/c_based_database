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
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_SYNTAX_ERROR
} PrepareResult;

typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
  STATEMENT_DELETE,
  STATEMENT_INSERT_SELECT,
  STATEMENT_BEGIN,
  STATEMENT_COMMIT,
  STATEMENT_ROLLBACK
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

  // For INSERT INTO ... SELECT ...
  char select_source_table[32];
  int select_has_where;
  char select_where_column[32];
  char select_where_operator[3];
  char select_where_value[255];
} Statement;

typedef struct Table Table;

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table);
PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement);

#endif
