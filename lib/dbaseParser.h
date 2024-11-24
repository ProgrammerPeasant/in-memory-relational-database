#ifndef PARSER_H
#define PARSER_H

#include <string>
#include <vector>
#include <unordered_set>

namespace DatabaseParser {

    enum class DataType {
        INT32,
        BOOL,
        STRING,
        BYTES
    };

    struct TypeDefinition {
        DataType type;
        int size;
        TypeDefinition() : type(DataType::INT32), size(0) {}
    };

    enum class Attribute {
        KEY,
        AUTOINCREMENT,
        UNIQUE
    };

    struct ColumnDefinition {
        std::unordered_set<Attribute> attributes;
        std::string name;
        TypeDefinition type;
        std::string defaultValue;
    };

    struct CreateTableStatement {
        std::string tableName;
        std::vector<ColumnDefinition> columns;
    };

    class Parser {
    public:
        explicit Parser(std::string  input);
        CreateTableStatement parseCreateTable();

    private:
        std::string m_input;
        size_t m_pos;

        void skipWhitespace();
        bool matchKeyword(const std::string& keyword);
        std::string parseIdentifier();
        std::unordered_set<Attribute> parseAttributes();
        TypeDefinition parseType();
        std::string parseDefaultValue();
        ColumnDefinition parseColumnDefinition();
    };

} // namespace DatabaseParser

#endif // PARSER_H