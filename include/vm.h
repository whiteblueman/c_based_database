#ifndef VM_H
#define VM_H

#include "compiler.h"
#include "table.h"

typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } ExecuteResult;

ExecuteResult execute_statement(Statement *statement, Table *table);

#endif
