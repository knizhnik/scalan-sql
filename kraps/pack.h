#include <string.h>

#define PACK(x) memcpy(dst + size, &src.x, sizeof src.x), size += sizeof(src.x)
#define UNPACK(x) memcpy(&dst.x, src + size, sizeof dst.x), size += sizeof(dst.x)
#define PACK_STR(x) size = stpcpy(dst + size, src.x) - dst + 1
#define UNPACK_STR(x) size = stpcpy(dst.x, src + size) - src + 1
