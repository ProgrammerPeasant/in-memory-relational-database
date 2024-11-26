#include "Dbase.h"
#include <iostream>
#include <stdexcept>
#include <cctype>
#include <algorithm>
#include <utility>

namespace Database {

    Database::Database() = default;

    void Database::createTable(const DatabaseParser::CreateTableStatement& createStmt) {
        if (tables_.find(createStmt.tableName) != tables_.end()) {
            throw std::runtime_error("Table already exists: " + createStmt.tableName);
        }
        tables_.emplace(createStmt.tableName, Table(createStmt.tableName, createStmt.columns));
    }

    void Database::insert(const std::string& insertStmt) {
        Parser parser(insertStmt);
        parser.parseInsert(*this);
    }

    void Database::select(const std::string& selectStmt) {
        Parser parser(selectStmt);
        parser.parseSelect(*this);
    }

    Table* Database::getTable(const std::string& name) {
        auto it = tables_.find(name);
        if (it != tables_.end()) {
            return &(it->second);
        }
        return nullptr;
    }

    Table::Table() = default;

    Table::Table(std::string  name, const std::vector<DatabaseParser::ColumnDefinition>& columns)
            : name_(std::move(name)), columns_(columns) {
        for (size_t i = 0; i < columns_.size(); ++i) {
            const auto& column = columns_[i];
            if (column.attributes.count(DatabaseParser::Attribute::AUTOINCREMENT)) {
                autoincrementCounters_[i] = 0;
            }
        }
    }

    void Table::insertRow(const std::vector<std::optional<Value>>& values) {
        if (values.size() != columns_.size()) {
            throw std::runtime_error("Column count doesn't match value count for table " + name_);
        }

        Row row;
        for (size_t i = 0; i < columns_.size(); ++i) {
            std::optional<Value> value = values[i];

            if (!value.has_value()) {
                if (columns_[i].attributes.count(DatabaseParser::Attribute::AUTOINCREMENT)) {
                    int32_t& counter = autoincrementCounters_[i];
                    value = counter++;
                } else {
                    value = getDefaultValue(i);
                }
            }

            if (!value.has_value()) {
                throw std::runtime_error("No value provided for column " + columns_[i].name);
            }

            row.push_back(value.value());
        }
        rows_.push_back(row);
    }

    const std::string& Table::getName() const {
        return name_;
    }

    const std::vector<DatabaseParser::ColumnDefinition>& Table::getColumns() const {
        return columns_;
    }

    const std::vector<Row>& Table::getRows() const {
        return rows_;
    }

    std::optional<Value> Table::getDefaultValue(size_t columnIndex) {
        const auto& colDef = columns_[columnIndex];
        if (!colDef.defaultValue.empty()) {
            const auto& typeDef = colDef.type;
            switch (typeDef.type) {
                case DatabaseParser::DataType::INT32:
                    return std::stoi(colDef.defaultValue);
                case DatabaseParser::DataType::BOOL:
                    return colDef.defaultValue == "true";
                case DatabaseParser::DataType::STRING:
                    return colDef.defaultValue;
                case DatabaseParser::DataType::BYTES:
                    return std::vector<uint8_t>{};
                default:
                    return std::nullopt;
            }
        }
        return std::nullopt;
    }

    Parser::Parser(std::string  input)
            : input_(std::move(input)), pos_(0) {}

    void Parser::parseInsert(Database& db) {
        skipWhitespace();
        if (!matchKeyword("insert")) {
            throw std::runtime_error("Expected 'insert' keyword");
        }
        skipWhitespace();
        if (input_[pos_] != '(') {
            throw std::runtime_error("Expected '(' after 'insert'");
        }
        ++pos_;
        skipWhitespace();

        bool namedValues = false;
        size_t tempPos = pos_;
        while (tempPos < input_.size() && input_[tempPos] != ')') {
            if (input_[tempPos] == '=') {
                namedValues = true;
                break;
            }
            ++tempPos;
        }

        std::string tableName;
        std::vector<std::optional<Value>> values;
        std::vector<std::string> columnNames;

        if (namedValues) {
            while (true) {
                skipWhitespace();
                std::string colName = parseIdentifier();
                skipWhitespace();
                if (input_[pos_] != '=') {
                    throw std::runtime_error("Expected '=' after column name");
                }
                ++pos_;
                skipWhitespace();
                columnNames.push_back(colName);
                auto value = parseStringLiteral();
                values.emplace_back(value);
                skipWhitespace();
                if (input_[pos_] == ',') {
                    ++pos_;
                } else {
                    break;
                }
            }
        } else {
            while (true) {
                skipWhitespace();
                if (input_[pos_] == ',') {
                    // Empty value
                    values.emplace_back(std::nullopt);
                    ++pos_;
                    continue;
                }
                if (input_[pos_] == ')') {
                    break;
                }
                // store value as string to be converted later
                auto value = parseStringLiteral();
                values.emplace_back(value);
                skipWhitespace();
                if (input_[pos_] == ',') {
                    ++pos_; // Skip ','
                } else if (input_[pos_] == ')') {
                    break;
                } else {
                    throw std::runtime_error("Expected ',' or ')' in value list");
                }
            }
        }

        if (input_[pos_] != ')') {
            throw std::runtime_error("Expected ')' after values");
        }
        ++pos_;

        skipWhitespace();
        if (!matchKeyword("to")) {
            throw std::runtime_error("Expected 'to' keyword");
        }
        skipWhitespace();
        tableName = parseIdentifier();

        Table* table = db.getTable(tableName);
        if (!table) {
            throw std::runtime_error("Table not found: " + tableName);
        }

        std::vector<std::optional<Value>> fullValues(table->getColumns().size(), std::nullopt);
        if (namedValues) {
            for (size_t i = 0; i < columnNames.size(); ++i) {
                const auto& colName = columnNames[i];
                auto it = std::find_if(table->getColumns().begin(), table->getColumns().end(),
                                       [&colName](const DatabaseParser::ColumnDefinition& colDef) {
                                           return colDef.name == colName;
                                       });
                if (it == table->getColumns().end()) {
                    throw std::runtime_error("Column not found: " + colName);
                }
                size_t colIndex = std::distance(table->getColumns().begin(), it);
                fullValues[colIndex] = values[i];
            }
        } else {
            if (values.size() > table->getColumns().size()) {
                throw std::runtime_error("Too many values for table " + tableName);
            }
            for (size_t i = 0; i < values.size(); ++i) {
                fullValues[i] = values[i];
            }
        }

        table->insertRow(fullValues);
    }

    void Parser::parseSelect(Database& db) {
        skipWhitespace();
        if (!matchKeyword("select")) {
            throw std::runtime_error("Expected 'select' keyword");
        }
        skipWhitespace();

        // parse columns
        std::vector<std::string> columns;
        while (true) {
            std::string col = parseIdentifier();
            columns.push_back(col);
            skipWhitespace();
            if (input_[pos_] == ',') {
                ++pos_;
                skipWhitespace();
            } else {
                break;
            }
        }

        if (!matchKeyword("from")) {
            throw std::runtime_error("Expected 'from' keyword");
        }
        skipWhitespace();

        // parse table name
        std::string tableName = parseIdentifier();

        Table* table = db.getTable(tableName);
        if (!table) {
            throw std::runtime_error("Table not found: " + tableName);
        }

        // parse optional 'where' clause
        // for now I haven't implemented condition parsing here

        const auto& allColumns = table->getColumns();
        const auto& rows = table->getRows();

        std::vector<size_t> colIndices;
        for (const auto& colName : columns) {
            auto it = std::find_if(allColumns.begin(), allColumns.end(),
                                   [&colName](const DatabaseParser::ColumnDefinition& colDef) {
                                       return colDef.name == colName;
                                   });
            if (it == allColumns.end()) {
                throw std::runtime_error("Column not found: " + colName);
            }
            colIndices.push_back(std::distance(allColumns.begin(), it));
        }

        for (const auto& row : rows) {
            for (size_t idx : colIndices) {
                const auto& value = row[idx];
                if (std::holds_alternative<int32_t>(value)) {
                    std::cout << std::get<int32_t>(value) << "\t";
                } else if (std::holds_alternative<bool>(value)) {
                    std::cout << (std::get<bool>(value) ? "true" : "false") << "\t";
                } else if (std::holds_alternative<std::string>(value)) {
                    std::cout << std::get<std::string>(value) << "\t";
                } else if (std::holds_alternative<std::vector<uint8_t>>(value)) {
                    std::cout << "[bytes]" << "\t";
                }
            }
            std::cout << "\n";
        }
    }

    void Parser::skipWhitespace() {
        while (pos_ < input_.size() && std::isspace(input_[pos_])) {
            ++pos_;
        }
    }

    bool Parser::matchKeyword(const std::string& keyword) {
        size_t tempPos = pos_;
        for (char c : keyword) {
            if (tempPos >= input_.size() || std::tolower(input_[tempPos]) != std::tolower(c)) {
                return false;
            }
            ++tempPos;
        }
        if (tempPos < input_.size() && std::isalnum(input_[tempPos])) {
            return false; // not a full match, part of a longer identifier
        }
        pos_ = tempPos;
        return true;
    }

    std::string Parser::parseIdentifier() {
        skipWhitespace();
        size_t start = pos_;
        while (pos_ < input_.size() && (std::isalnum(input_[pos_]) || input_[pos_] == '_')) {
            ++pos_;
        }
        if (start == pos_) {
            throw std::runtime_error("Expected identifier at position " + std::to_string(pos_));
        }
        return input_.substr(start, pos_ - start);
    }

    std::string Parser::parseStringLiteral() {
        skipWhitespace();
        if (input_[pos_] == '"') {
            ++pos_;
            size_t start = pos_;
            while (pos_ < input_.size() && input_[pos_] != '"') {
                ++pos_;
            }
            if (pos_ == input_.size()) {
                throw std::runtime_error("Unterminated string literal");
            }
            std::string result = input_.substr(start, pos_ - start);
            ++pos_;
            return result;
        } else if (input_[pos_] == '0' && input_[pos_ + 1] == 'x') {
            pos_ += 2; // Skip '0x'
            size_t start = pos_;
            while (pos_ < input_.size() && std::isxdigit(input_[pos_])) {
                ++pos_;
            }
            return "0x" + input_.substr(start, pos_ - start);
        } else {
            size_t start = pos_;
            while (pos_ < input_.size() && !std::isspace(input_[pos_]) && input_[pos_] != ',' && input_[pos_] != ')') {
                ++pos_;
            }
            return input_.substr(start, pos_ - start);
        }
    }

    Value Parser::parseValue(const DatabaseParser::TypeDefinition& typeDef) {
        std::string valueStr = parseStringLiteral();
        switch (typeDef.type) {
            case DatabaseParser::DataType::INT32:
                return std::stoi(valueStr);
            case DatabaseParser::DataType::BOOL:
                return valueStr == "true";
            case DatabaseParser::DataType::STRING:
                return valueStr;
            case DatabaseParser::DataType::BYTES:
                return std::vector<uint8_t>{};
            default:
                throw std::runtime_error("Unknown data type");
        }
    }

}