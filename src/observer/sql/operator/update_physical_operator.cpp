#include "sql/operator/update_physical_operator.h"
#include "sql/stmt/update_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

// 执行update操作
RC UpdatePhysicalOperator::open(Trx *trx)
{
    // 1. 没有孩子结点
    if(children_.empty()) {
        return RC::SUCCESS;
    }

    // 2. 取一个孩子结点
    std::unique_ptr<PhysicalOperator> &child = children_[0];
    RC rc = child->open(trx);
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to open child operator: %s", strrc(rc));
        return rc;
    }

    // 3. 给transacion赋值
    trx_ = trx;

    return RC::SUCCESS;
}

// 开始一个一个更新
RC UpdatePhysicalOperator::next()
{
    // 1. 没有孩子结点
    RC rc = RC::SUCCESS;
    if (children_.empty()) {
        return RC::RECORD_EOF;
    }

    // 2. 取一个孩子结点，并循环取record做update操作
    auto &child = children_[0];
    while(RC::SUCCESS == (rc = child->next())) {
        Tuple *tuple = child->current_tuple();
        if(nullptr == tuple) {
            LOG_WARN("failed to get current record: %s", strrc(rc));
            return rc;
        }

        // 2.1 取old record
        RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
        Record &old_record = row_tuple->record();

        // 2.2 创建new record
        Record new_record;
            // 复制所有字段的值
            // 这里取到的row_tuple是没有设置len的，无语了。。。家人们
        // int record_size = row_tuple->record().len();
        int record_size = table_->table_meta().record_size();
        char *record_data = (char *)malloc(record_size);
        memcpy(record_data, row_tuple->record().data(), record_size);
            // 修改set的field
        size_t copy_len = field_meta_->len();
        if (field_meta_->type() == CHARS) {
            const size_t data_len = value_->length();
            if (copy_len > data_len) {  // 防止memcpy的时候，value+copy_len溢出
                copy_len = data_len + 1;  
            }
        }
        memcpy(record_data + field_meta_->offset(), value_->data(), copy_len);
            // set new_record
        new_record.set_data_owner(record_data, record_size);

        // 2.3 执行update（有可能失败，因为可能有索引的键重复，重复后update函数会自动恢复）
        rc = trx_->update_record(table_, old_record, new_record);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to update record by transaction. rc=%s", strrc(rc));
            return rc;
        }
    }
    // 3. 返回EOF，只调用一次
    return RC::RECORD_EOF;
}

// 直接返回SUCCESS
RC UpdatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}