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

  if (pager->num_pages == 0) {
    // New database file. Initialize page 0 as Meta Page.
    // Page 1 as Main Table Root.
    // Page 3 as Orders Table Root.
    void *meta_page = get_page(pager, 0);
    void *main_root_node = get_page(pager, 1);
    void *index_root_node = get_page(pager, 2);
    void *orders_root_node = get_page(pager, 3);

    initialize_leaf_node(main_root_node);
    set_node_root(main_root_node, true);

    initialize_leaf_node(index_root_node);
    set_node_root(index_root_node, true);

    initialize_leaf_node(orders_root_node);
    set_node_root(orders_root_node, true);

    // Write root page numbers to Meta Page
    *(uint32_t *)((char *)meta_page + 0) = 1; // Main Root
    *(uint32_t *)((char *)meta_page + 4) = 2; // Index Root
    *(uint32_t *)((char *)meta_page + 8) = 3; // Orders Root

    table->main_root_page_num = 1;
    table->index_root_page_num = 2;
    table->orders_root_page_num = 3;

    // Flush these pages to disk so we have a valid base state for Rollback
    pager_flush(pager, 0, PAGE_SIZE);
    pager_flush(pager, 1, PAGE_SIZE);
    pager_flush(pager, 2, PAGE_SIZE);
    pager_flush(pager, 3, PAGE_SIZE);
  } else {
    // Existing database
    void *meta_page = get_page(pager, 0);
    table->main_root_page_num = *(uint32_t *)((char *)meta_page + 0);
    table->index_root_page_num = *(uint32_t *)((char *)meta_page + 4);
    table->orders_root_page_num = *(uint32_t *)((char *)meta_page + 8);
  }

  return table;
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

  void *meta_page = get_page(pager, 0);
  *(uint32_t *)((char *)meta_page + 0) = table->main_root_page_num;
  *(uint32_t *)((char *)meta_page + 4) = table->index_root_page_num;
  *(uint32_t *)((char *)meta_page + 8) = table->orders_root_page_num;

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
