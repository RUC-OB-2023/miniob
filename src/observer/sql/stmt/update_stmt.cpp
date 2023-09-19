/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

UpdateStmt::UpdateStmt(Table *table, const FieldMeta *field_meta, const Value *values, int value_amount, FilterStmt *filter_stmt)
    : table_(table), field_meta_(field_meta), values_(values), value_amount_(value_amount), filter_stmt_(filter_stmt)
{}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  // 1. 检查update语句语法
  const char *table_name = update.relation_name.c_str();
  const char *field_name = update.attribute_name.c_str();
  const Value &value = update.value;
  if(nullptr == db || nullptr == table_name || nullptr == field_name || UNDEFINED == value.attr_type()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, field_name=%p, value.type=%d",
        db, table_name, field_name, value.attr_type());
    return RC::INVALID_ARGUMENT;
  }

  // 2. 检查表是否存在
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // 3. 检查列名是否存在
  const TableMeta& table_meta = table->table_meta();
  const FieldMeta *field_meta = table_meta.field(field_name);
  if(nullptr == field_meta) {
    LOG_WARN("field name not found. field name=%s", field_name);
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  // 4. 检查列的类型是否匹配
  const AttrType field_type = field_meta->type();
  const AttrType value_type = value.attr_type();
  if(field_type != value_type) { // TODO try to convert the value type to field type
      LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
          table_name, field_meta->name(), field_type, value_type);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  // 5. 获取filter stmt
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if(rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  // 6. 创建Update
  stmt = new UpdateStmt(table, field_meta, &value, 1, filter_stmt);
  return RC::SUCCESS;
}
