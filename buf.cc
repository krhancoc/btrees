#include "buf.h"

#include <unordered_map>

std::mutex buffer_cache_lk;
std::unordered_map<uint64_t,struct buf *> buffer_cache;

std::atomic<uint64_t> pblkno;

struct buf *
getblk(uint64_t lblkno, size_t size)
{
  std::lock_guard<std::mutex> guard(buffer_cache_lk);
  auto iter = buffer_cache.find(lblkno);
  if (iter == buffer_cache.end()) {
    struct buf *bp = (struct buf *)malloc(sizeof(struct buf));
    bp->bp_data = malloc(size);
    bzero(bp->bp_data, size);
    bp->bp_data = malloc(size);
    bp->bp_lblkno = lblkno;

    buffer_cache.insert({lblkno, bp});
    return bp;
  }

  return iter->second;
}


diskptr_t
allocate_blk(size_t size)
{
  diskptr_t ptr;
  size = ((size + (PBLKSZ - 1)) / PBLKSZ);
  ptr.size = size;
  auto off = pblkno.fetch_add(size);
  ptr.offset = off;
  ptr.epoch = 0;
  return ptr;
}
