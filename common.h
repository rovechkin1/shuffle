//
// Created by ruslan on 10/4/21.
//

#ifndef SHUFFLE_COMMON_H
#define SHUFFLE_COMMON_H
constexpr int MAX_WORK_BUF  = 1<<23; // about 8 MB
constexpr int MSG_HEADER_LEN  = 4; // message header length

using byte = unsigned char;
using part_type = int;
#endif //SHUFFLE_COMMON_H
