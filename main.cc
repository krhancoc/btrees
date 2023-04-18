#include <stdio.h>
#include <random>
#include <cstdint>
#include <unordered_map>
#include <cassert>
#include <string.h>

#include "buf.h"
#include "btree.h"

uint64_t generate_unique_key()
{
  static std::mt19937_64 rng(std::random_device{}());
  static std::uniform_int_distribution<uint64_t> dist;

  uint64_t key;
  do {
      key = dist(rng);
  } while (key == 0);
  return key;
}

diskptr_t generate_diskptr()
{
  diskptr_t ptr;
  ptr.size = generate_unique_key();
  ptr.offset = generate_unique_key();
  ptr.epoch = generate_unique_key();
  return ptr;
}


#define MAX_KEYS (10000000)

int main(int argc, char *argv[])
{
  std::unordered_map<uint64_t, diskptr_t> keys;
  diskptr_t value;
  btree tree;
  int error;
  diskptr_t ptr = allocate_blk(BLKSZ);

  error = btree_init(&tree, ptr, sizeof(ptr));
  if (error) {
    printf("Problem initing\n");
  }

  for (uint64_t i = 0; i < MAX_KEYS; i++) {
    uint64_t key = generate_unique_key();
    while (keys.find(key) != keys.end()) {
      key = generate_unique_key();
    }
    diskptr_t value = generate_diskptr();
    btree_insert(&tree, key, &value);
    diskptr_t check;
    error = btree_find(&tree, key, &check);
    assert(error == 0);
    assert(memcmp(&check, &value, sizeof(diskptr_t)) == 0);
  }

  return 0;
}
