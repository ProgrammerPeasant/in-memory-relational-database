#include "dbaseParser.h"
#include <cctype>
#include <algorithm>
#include <stdexcept>
#include <iostream>
#include <utility>

namespace DatabaseParser {

    Parser::Parser(std::string  input)
            : m_input(std::move(input)), m_pos(0) {}

    void Parser::skipWhitespace() {
        while (m_pos < m_input.length() && std::isspace(m_input[m_pos])) {
            ++m_pos;
        }
    }

    bool Parser::matchKeyword(const std::string& keyword) {
        skipWhitespace();
        size_t len = keyword.length();

        if (m_pos + len <= m_input.length()) {
            std::string word = m_input.substr(m_pos, len);
            std::transform(word.begin(), word.end(), word.begin(), ::tolower);
            std::string keywordLower = keyword;
            std::transform(keywordLower.begin(), keywordLower.end(), keywordLower.begin(), ::tolower);

            if (word == keywordLower) {
                m_pos += len;
                return true;
            }
        }
        return false;
    }

    std::string Parser::parseIdentifier() {
        skipWhitespace();
        size_t start = m_pos;
        while (m_pos < m_input.length() && (std::isalnum(m_input[m_pos]) || m_input[m_pos] == '_')) {
            ++m_pos;
        }
        if (start == m_pos) {
            throw std::runtime_error("Expected an identifier at position " + std::to_string(m_pos));
        }
        return m_input.substr(start, m_pos - start);
    }

    std::unordered_set<Attribute> Parser::parseAttributes() {
        skipWhitespace();
        std::unordered_set<Attribute> attributes;
        if (m_pos < m_input.length() && m_input[m_pos] == '{') {
            ++m_pos;
            skipWhitespace();
            while (true) {
                size_t start = m_pos;
                while (m_pos < m_input.length() && std::isalpha(m_input[m_pos])) {
                    ++m_pos;
                }
                if (start == m_pos) {
                    throw std::runtime_error("Expected attribute name at position " + std::to_string(m_pos));
                }
                std::string attr = m_input.substr(start, m_pos - start);
                std::transform(attr.begin(), attr.end(), attr.begin(), ::tolower);
                if (attr == "key") {
                    attributes.insert(Attribute::KEY);
                } else if (attr == "autoincrement") {
                    attributes.insert(Attribute::AUTOINCREMENT);
                } else if (attr == "unique") {
                    attributes.insert(Attribute::UNIQUE);
                } else {
                    throw std::runtime_error("Unknown attribute '" + attr + "' at position " + std::to_string(start));
                }
                skipWhitespace();
                if (m_pos < m_input.length() && m_input[m_pos] == ',') {
                    ++m_pos;
                    skipWhitespace();
                } else {
                    break;
                }
            }
            if (m_pos >= m_input.length() || m_input[m_pos] != '}') {
                throw std::runtime_error("Expected '}' at position " + std::to_string(m_pos));
            }
            ++m_pos;
        }
        return attributes;
    }

    TypeDefinition Parser::parseType() {
        skipWhitespace();
        TypeDefinition typeDef;
        size_t start = m_pos;
        while (m_pos < m_input.length() && (std::isalnum(m_input[m_pos]) || m_input[m_pos] == '_')) {
            ++m_pos;
        }
        if (start == m_pos) {
            throw std::runtime_error("Expected data type at position " + std::to_string(m_pos));
        }
        std::string typeName = m_input.substr(start, m_pos - start);
        std::transform(typeName.begin(), typeName.end(), typeName.begin(), ::tolower);
        if (typeName == "int32") {
            typeDef.type = DataType::INT32;
        } else if (typeName == "bool") {
            typeDef.type = DataType::BOOL;
        } else if (typeName == "string") {
            typeDef.type = DataType::STRING;
        } else if (typeName == "bytes") {
            typeDef.type = DataType::BYTES;
        } else {
            throw std::runtime_error("Unknown data type '" + typeName + "' at position " + std::to_string(start));
        }

        skipWhitespace();
        if (m_pos < m_input.length() && m_input[m_pos] == '[') {
            ++m_pos;
            skipWhitespace();
            size_t sizeStart = m_pos;
            while (m_pos < m_input.length() && std::isdigit(m_input[m_pos])) {
                ++m_pos;
            }
            if (sizeStart == m_pos) {
                throw std::runtime_error("Expected type size at position " + std::to_string(m_pos));
            }
            std::string sizeStr = m_input.substr(sizeStart, m_pos - sizeStart);
            typeDef.size = std::stoi(sizeStr);
            skipWhitespace();
            if (m_pos >= m_input.length() || m_input[m_pos] != ']') {
                throw std::runtime_error("Expected ']' at position " + std::to_string(m_pos));
            }
            ++m_pos;
        }

        return typeDef;
    }

    std::string Parser::parseDefaultValue() {
        skipWhitespace();
        if (m_pos >= m_input.length() || m_input[m_pos] != '=') {
            return "";
        }
        ++m_pos;
        skipWhitespace();
        size_t start = m_pos;
        while (m_pos < m_input.length() && m_input[m_pos] != ',' && m_input[m_pos] != ')') {
            ++m_pos;
        }
        return m_input.substr(start, m_pos - start);
    }

    ColumnDefinition Parser::parseColumnDefinition() {
        ColumnDefinition column;
        column.attributes = parseAttributes();
        skipWhitespace();
        column.name = parseIdentifier();
        skipWhitespace();
        if (m_pos >= m_input.length() || m_input[m_pos] != ':') {
            throw std::runtime_error("Expected ':' after column name at position " + std::to_string(m_pos));
        }
        ++m_pos;
        column.type = parseType();
        column.defaultValue = parseDefaultValue();
        return column;
    }

    CreateTableStatement Parser::parseCreateTable() {
        skipWhitespace();
        if (!matchKeyword("create")) {
            throw std::runtime_error("Expected keyword 'CREATE' at position " + std::to_string(m_pos));
        }
        if (!matchKeyword("table")) {
            throw std::runtime_error("Expected keyword 'TABLE' after 'CREATE' at position " + std::to_string(m_pos));
        }
        skipWhitespace();
        std::string tableName = parseIdentifier();
        skipWhitespace();
        if (m_pos >= m_input.length() || m_input[m_pos] != '(') {
            throw std::runtime_error("Expected '(' after table name at position " + std::to_string(m_pos));
        }
        ++m_pos;
        skipWhitespace();

        CreateTableStatement createStmt;
        createStmt.tableName = tableName;

        while (true) {
            ColumnDefinition column = parseColumnDefinition();
            createStmt.columns.push_back(column);
            skipWhitespace();
            if (m_pos >= m_input.length()) {
                throw std::runtime_error("Unexpected end of input inside column definitions");
            }
            if (m_input[m_pos] == ',') {
                ++m_pos;
                skipWhitespace();
            } else if (m_input[m_pos] == ')') {
                ++m_pos;
                break;
            } else {
                throw std::runtime_error("Expected ',' or ')' in column definitions at position " + std::to_string(m_pos));
            }
        }
        return createStmt;
    }

} // namespace DatabaseParser
