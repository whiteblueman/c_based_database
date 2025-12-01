#include "table.h"
#include "node.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

Table *db_open(const char *filename) {
  Pager *pager = pager_open(filename);

  Table *table = malloc(sizeof(Table));
  table->pager = pager;
  table->num_rows = 0; // Unused mostly
  table->in_transaction = false;

  if (pager->num_pages == 0) {
    // New database file. Initialize page 0 as Meta Page.
    // Page 1: Main Table (users) Root
    // Page 2: Index Root
    // Page 3: Orders Table Root
    // Page 4: Directory Table Root

    void *meta_page = get_page(pager, 0);
    void *main_root_node = get_page(pager, 1);
    void *index_root_node = get_page(pager, 2);
    void *orders_root_node = get_page(pager, 3);
    void *directory_root_node = get_page(pager, 4);

    initialize_leaf_node(main_root_node);
    set_node_root(main_root_node, true);

    initialize_leaf_node(index_root_node);
    set_node_root(index_root_node, true);

    initialize_leaf_node(orders_root_node);
    set_node_root(orders_root_node, true);

    initialize_leaf_node(directory_root_node);
    set_node_root(directory_root_node, true);

    // Write root page numbers to Meta Page (Legacy + Directory)
    *(uint32_t *)((char *)meta_page + 0) = 1;  // Main Root (Legacy)
    *(uint32_t *)((char *)meta_page + 4) = 2;  // Index Root
    *(uint32_t *)((char *)meta_page + 8) = 3;  // Orders Root
    *(uint32_t *)((char *)meta_page + 12) = 4; // Directory Root

    table->directory_root_page_num = 4;

    // Initialize default tables in memory
    table->num_tables = 2;
    strcpy(table->tables[0].name, "users");
    table->tables[0].root_page_num = 1;
    table->tables[0].schema_type = 0; // User

    strcpy(table->tables[1].name, "orders");
    table->tables[1].root_page_num = 3;
    table->tables[1].schema_type = 1; // Order

    // Persist default tables to Directory Table
    // For simplicity, we are NOT implementing full B-Tree insertion for
    // Directory yet. We will just rely on in-memory reconstruction for
    // defaults, and only persist NEW tables. ACTUALLY, to be robust, we should
    // insert them. But since we don't have a generic insert for arbitrary
    // structs easily available without casting... Let's just assume for Phase
    // 15 that we start with these defaults in memory. New tables will be added
    // to memory AND persisted if we implement that. For now, let's stick to
    // in-memory + Meta Page for roots. Wait, the plan said "Implement Directory
    // Table to persist table metadata". Let's use Page 4 as a simple array of
    // TableInfo for now (not B-Tree) or just use B-Tree if we can. Given the
    // complexity, let's use Page 4 as a flat storage for TableInfo structs for
    // now? No, let's try to use the B-Tree if possible, but our B-Tree is typed
    // (Row or OrderRow). It's hard to reuse it for TableInfo without generic
    // support. ALTERNATIVE: Just use Page 4 as a flat file of TableInfo
    // structs. It's a single page, can hold 4096 / sizeof(TableInfo) tables.
    // sizeof(TableInfo) = 32 + 4 + 4 = 40 bytes. ~100 tables. Enough.

    // Write defaults to Page 4
    memcpy((char *)directory_root_node, table->tables,
           sizeof(TableInfo) * table->num_tables);
    *(uint32_t *)((char *)directory_root_node + PAGE_SIZE - 4) =
        table->num_tables; // Store count at end

    pager_flush(pager, 0, PAGE_SIZE);
    pager_flush(pager, 1, PAGE_SIZE);
    pager_flush(pager, 2, PAGE_SIZE);
    pager_flush(pager, 3, PAGE_SIZE);
    pager_flush(pager, 4, PAGE_SIZE);

  } else {
    // Existing database
    void *meta_page = get_page(pager, 0);
    // Legacy loads
    // table->main_root_page_num = *(uint32_t *)((char *)meta_page + 0);

    // Load Directory Root
    table->directory_root_page_num = *(uint32_t *)((char *)meta_page + 12);
    if (table->directory_root_page_num == 0) {
      // Migration for existing DBs that didn't have directory
      table->directory_root_page_num = 4;
      // Initialize defaults
      table->num_tables = 2;
      strcpy(table->tables[0].name, "users");
      table->tables[0].root_page_num = *(uint32_t *)((char *)meta_page + 0);
      table->tables[0].schema_type = 0;
      strcpy(table->tables[1].name, "orders");
      table->tables[1].root_page_num = *(uint32_t *)((char *)meta_page + 8);
      table->tables[1].schema_type = 1;

      // Save to page 4
      void *dir_page = get_page(pager, 4);
      memcpy((char *)dir_page, table->tables,
             sizeof(TableInfo) * table->num_tables);
      *(uint32_t *)((char *)dir_page + PAGE_SIZE - 4) = table->num_tables;

      *(uint32_t *)((char *)meta_page + 12) = 4; // Update meta
      pager_flush(pager, 0, PAGE_SIZE);
      pager_flush(pager, 4, PAGE_SIZE);
    } else {
      // Load from Directory Page
      void *dir_page = get_page(pager, table->directory_root_page_num);
      table->num_tables = *(uint32_t *)((char *)dir_page + PAGE_SIZE - 4);
      memcpy(table->tables, (char *)dir_page,
             sizeof(TableInfo) * table->num_tables);
    }
  }

  return table;
}

TableInfo *find_table(Table *table, const char *name) {
  for (uint32_t i = 0; i < table->num_tables; i++) {
    if (strcmp(table->tables[i].name, name) == 0) {
      return &table->tables[i];
    }
  }
  return NULL;
}

void db_close(Table *table) {
  Pager *pager = table->pager;

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    if (pager->pages[i]) {
      pager_flush(pager, i, PAGE_SIZE);
      free(pager->pages[i]);
      pager->pages[i] = NULL;
    }
  }

  // Explicitly flush page 0 if it wasn't in the cache (though it should be if
  // we opened it) Actually, the loop above covers it if it's in pager->pages.
  // But let's make sure we update the root page numbers in the meta page before
  // flushing. We need to write table->main_root_page_num and
  // table->index_root_page_num to page 0.

  // Flush Directory Page
  void *dir_page = get_page(pager, table->directory_root_page_num);
  memcpy((char *)dir_page, table->tables,
         sizeof(TableInfo) * table->num_tables);
  *(uint32_t *)((char *)dir_page + PAGE_SIZE - 4) = table->num_tables;
  pager_flush(pager, table->directory_root_page_num, PAGE_SIZE);

  void *meta_page = get_page(pager, 0);
  // We don't strictly need to update these legacy fields if we use directory,
  // but let's keep them for safety if we ever revert.
  // table->main_root_page_num is gone from struct, so we can't.
  // Just update directory root.
  *(uint32_t *)((char *)meta_page + 12) = table->directory_root_page_num;

  // Now flush page 0 again to be sure
  pager_flush(pager, 0, PAGE_SIZE);

  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void *page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}

void *row_slot(Table *table, uint32_t row_num) {
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void *page = get_page(table->pager, page_num);
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;
  return (char *)page + byte_offset;
}

void serialize_row(Row *source, void *destination) {
  memcpy((char *)destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy((char *)destination + USERNAME_OFFSET, &(source->username),
         USERNAME_SIZE);
  memcpy((char *)destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
  memcpy(&(destination->id), (char *)source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), (char *)source + USERNAME_OFFSET,
         USERNAME_SIZE);
  memcpy(&(destination->email), (char *)source + EMAIL_OFFSET, EMAIL_SIZE);
}
