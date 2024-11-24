#include <iostream>
#include "dbaseParser.h"

int main() {
    std::string input = R"(create table users ({key, autoincrement} id: int32, {unique} login: string[32], password_hash: bytes[8], is_admin: bool = false))";

    try {
        DatabaseParser::Parser parser(input);
        DatabaseParser::CreateTableStatement createStmt = parser.parseCreateTable();

        std::cout << "Table Name: " << createStmt.tableName << std::endl;
        for (const auto& column : createStmt.columns) {
            std::cout << "Column Name: " << column.name << std::endl;
            std::cout << "Type: ";
            switch (column.type.type) {
                case DatabaseParser::DataType::INT32:
                    std::cout << "int32";
                    break;
                case DatabaseParser::DataType::BOOL:
                    std::cout << "bool";
                    break;
                case DatabaseParser::DataType::STRING:
                    std::cout << "string" << column.type.size;
                    break;
                case DatabaseParser::DataType::BYTES:
                    std::cout << "bytes" << column.type.size;
                    break;
            }
            std::cout << std::endl;
            if (!column.attributes.empty()) {
                std::cout << "Attributes: ";
                for (const auto& attr : column.attributes) {
                    switch (attr) {
                        case DatabaseParser::Attribute::KEY:
                            std::cout << "key ";
                            break;
                        case DatabaseParser::Attribute::AUTOINCREMENT:
                            std::cout << "autoincrement ";
                            break;
                        case DatabaseParser::Attribute::UNIQUE:
                            std::cout << "unique ";
                            break;
                    }
                }
                std::cout << std::endl;
            }
            if (!column.defaultValue.empty()) {
                std::cout << "Default Value: " << column.defaultValue << std::endl;
            }
            std::cout << "------------------------" << std::endl;
        }

    } catch (const std::exception& e) {
        std::cerr << "Parsing error: " << e.what() << std::endl;
    }

    return 0;
}