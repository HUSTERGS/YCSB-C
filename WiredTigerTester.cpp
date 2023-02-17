//
// Created by song ge on 2023/2/16.
//

#include "wiredtiger.h"
#include <string>
#include <iostream>

#define KEY_LEN 8
#define VALUE_LEN 8
#define TEST_SIZE 10000

int main(int argc, char *argv[]) {
    const char * home = "/Users/samuel/git-repos/YCSB-C/data";
    WT_CONNECTION *conn = nullptr;
    WT_SESSION * session = nullptr;
    WT_CURSOR *cursor = nullptr;

    wiredtiger_open(home, nullptr, "create", &conn);
    conn->open_session(conn, nullptr, nullptr, &session);
    session->create(session, "table:basic_test", "key_format=S,value_format=S");
    session->open_cursor(session, "table:basic_test", nullptr, nullptr, &cursor);

    for (int i = 0; i < TEST_SIZE; i++) {
        cursor->set_key(cursor, std::to_string(i).c_str());
        cursor->set_value(cursor, std::to_string(i).c_str());
        cursor->insert(cursor);
    }

    for (int i = 0; i < TEST_SIZE; i++) {
        cursor->set_key(cursor, std::to_string(i).c_str());
        cursor->search(cursor);
        const char *value = nullptr;
        cursor->get_value(cursor, &value);
        if (value == nullptr || std::to_string(i) != std::string(value)) {
            std::cout << "error" << i << std::endl;
            break;
        }
    }

    conn->close(conn, nullptr);
    return 0;
}