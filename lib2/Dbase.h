#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>
#include <unordered_map>
#include <variant>
#include <optional>
#include <unordered_set>
#include <cstdint>
#include "dbaseParser.h"

namespace Database {

    using Value = std::variant<int32_t, bool, std::string, std::vector<uint8_t>>;

    using Row = std::vector<Value>;

    class Table;

    class Database {
    public:
        Database();

        void createTable(const DatabaseParser::CreateTableStatement& createStmt);

        void insert(const std::string& insertStmt);

        void select(const std::string& selectStmt);

        Table* getTable(const std::string& name);

    private:
        std::unordered_map<std::string, Table> tables_;
    };

    class Table {
    public:
        Table();
        Table(std::string  name, const std::vector<DatabaseParser::ColumnDefinition>& columns);

        void insertRow(const std::vector<std::optional<Value>>& values);

        const std::string& getName() const;
        const std::vector<DatabaseParser::ColumnDefinition>& getColumns() const;
        const std::vector<Row>& getRows() const;

    private:
        std::string name_;
        std::vector<DatabaseParser::ColumnDefinition> columns_;
        std::vector<Row> rows_;

        std::optional<Value> getDefaultValue(size_t columnIndex);

        std::unordered_map<size_t, int32_t> autoincrementCounters_;
    };

    class Parser {
    public:
        explicit Parser(std::string  input);

        void parseInsert(Database& db);
        void parseSelect(Database& db);

    private:
        std::string input_;
        size_t pos_;

        void skipWhitespace();
        bool matchKeyword(const std::string& keyword);
        std::string parseIdentifier();
        std::string parseStringLiteral();
        Value parseValue(const DatabaseParser::TypeDefinition& typeDef);
        std::vector<std::optional<Value>> parseValues(const Table& table, bool namedValues);

        // expression parsing for conditions will implement later

    };

}

#endif // DATABASE_H