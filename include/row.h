#ifndef ROW_H
#define ROW_H

#include <stdint.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
} Row;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

#define ID_SIZE size_of_attribute(Row, id)
#define USERNAME_SIZE size_of_attribute(Row, username)
#define EMAIL_SIZE size_of_attribute(Row, email)
#define ID_OFFSET 0
#define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)
#define EMAIL_OFFSET (USERNAME_OFFSET + USERNAME_SIZE)
#define ROW_SIZE (ID_SIZE + USERNAME_SIZE + EMAIL_SIZE)

#define COLUMN_PRODUCT_NAME_SIZE 32

typedef struct {
  uint32_t id;
  uint32_t user_id;
  char product_name[COLUMN_PRODUCT_NAME_SIZE + 1];
} OrderRow;

#define ORDER_ID_SIZE size_of_attribute(OrderRow, id)
#define ORDER_USER_ID_SIZE size_of_attribute(OrderRow, user_id)
#define ORDER_PRODUCT_NAME_SIZE size_of_attribute(OrderRow, product_name)

#define ORDER_ID_OFFSET 0
#define ORDER_USER_ID_OFFSET (ORDER_ID_OFFSET + ORDER_ID_SIZE)
#define ORDER_PRODUCT_NAME_OFFSET (ORDER_USER_ID_OFFSET + ORDER_USER_ID_SIZE)
#define ORDER_ROW_SIZE                                                         \
  (ORDER_ID_SIZE + ORDER_USER_ID_SIZE + ORDER_PRODUCT_NAME_SIZE)

#endif
