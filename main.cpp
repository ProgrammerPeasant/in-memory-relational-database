#include <iostream>
#include "Dbase.h"

int main() {

    std::string createStmtStr = R"(create table users ({key, autoincrement} id: int32, {unique} login: string[32], password_hash: bytes[8], is_admin: bool = false))";
    DatabaseParser::Parser createParser(createStmtStr);
    DatabaseParser::CreateTableStatement createStmt = createParser.parseCreateTable();

    // Create the database and create the table
    Database::Database db;
    Database::Database db1;
    db.createTable(createStmt);

    // Insert statements
    db.insert(R"(insert (,"vasya",0xdeadbeefdeadbeef) to users)");
    db.insert(R"(insert (login = "admin", password_hash = 0x0000000000000000, is_admin = true) to users)");

    // Select statement
    db.select("select id, login from users where is_admin || id < 10");

    db1.execute(R"(create table users ({key, autoincrement} id: int32, {unique} login: string[32], password_hash: bytes[8], is_admin: bool = false))");
    db1.execute(R"(insert (,"vasya",0xdeadbeefdeadbeef) to users)");
    db1.execute(R"(select id, login from users where is_admin || id < 10)");

    return 0;
}