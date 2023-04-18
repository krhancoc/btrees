#ifndef _BUF_H
#define _BUF_H

#include <sys/types.h>
#include <shared_mutex>

#define DPTR_COW (0x1)
#define DPTR_RDX_LEAF (0x2)
#define DPTR_RDX_INNER (0x4)
#define DPTR_DATA (0x8)

const uint64_t PBLKSZ = 4 * 1024;

typedef struct diskptr {
  uint64_t offset;
  uint64_t size;
  uint64_t epoch;
  uint16_t flags;
} diskptr_t;

struct buf {
  void                *bp_data;
  size_t              bp_lblkno;
  std::shared_mutex   bp_lk;
};

struct buf *getblk(uint64_t blkno, size_t size);
diskptr_t allocate_blk(size_t size);
#endif
