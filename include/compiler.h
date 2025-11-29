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

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef struct {
  StatementType type;
  Row row_to_insert;
  char where_column[32];
  char where_operator[3];
  char where_value[255];
  int has_where;
} Statement;

typedef struct Table Table;

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table);
PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement);

#endif
