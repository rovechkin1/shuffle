//
// Created by ruslan on 10/4/21.
//

#ifndef SHUFFLE_MOCK_SNAPPY_H
#define SHUFFLE_MOCK_SNAPPY_H

// temp plugin to imitate snappy
class SnappyMock {
public:
    static std::vector<unsigned char> compress(const std::vector<unsigned char> &in) {
        // does nothing
        return std::vector(in.begin(), in.end());
    }

    static std::vector<unsigned char> decompress(const std::vector<unsigned char> &in) {
        // does nothing
        return std::vector(in.begin(), in.end());
    }
};

#endif //SHUFFLE_MOCK_SNAPPY_H
