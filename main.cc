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


#define MAX_KEYS (1000000)

int main(int argc, char *argv[])
{
  std::unordered_map<uint64_t, diskptr_t> keys;
  diskptr_t value;
  diskptr_t check;
  btree tree;
  int error;
  diskptr_t ptr = allocate_blk(BLKSZ);

  error = btree_init(&tree, ptr, sizeof(ptr));
  if (error) {
    printf("Problem initing\n");
  }

  printf("Filling keys...\n");
  for (uint64_t i = 0; i < MAX_KEYS; i++) {
    uint64_t key = generate_unique_key();
    while (keys.find(key) != keys.end()) {
      key = generate_unique_key();
    }
    diskptr_t value = generate_diskptr();

    keys.insert({key, value});

    btree_insert(&tree, key, &value);
    error = btree_find(&tree, key, &check);
    assert(error == 0);
    assert(memcmp(&check, &value, sizeof(diskptr_t)) == 0);

    int onlyten = 0;
    for (auto t : keys) {
      error = btree_find(&tree, t.first, &check);
      assert(error == 0);
      assert(memcmp(&check, &t.second, sizeof(diskptr_t)) == 0);
      if (onlyten == 10) {
        break;
      }
      onlyten += 1;
    }
  }
  for (auto t : keys) {
    error = btree_find(&tree, t.first, &check);
    assert(error == 0);
    assert(memcmp(&check, &t.second, sizeof(diskptr_t)) == 0);
  }

  printf("Deleting keys...\n");
  while (!keys.empty()) {
    auto it = keys.begin();
    btree_delete(&tree, it->first, &check);
    assert(memcmp(&check, &it->second, sizeof(diskptr_t)) == 0);
    bzero(&check, sizeof(diskptr_t));
    error = btree_find(&tree, it->first, &check);
    assert(error != 0);
    keys.erase(it); 

    int onlyten = 0;
    for (auto t : keys) {
      error = btree_find(&tree, t.first, &check);
      assert(error == 0);
      assert(memcmp(&check, &t.second, sizeof(diskptr_t)) == 0);
      if (onlyten == 10) {
        break;
      }
      onlyten += 1;
    }
  }
  return 0;
}
