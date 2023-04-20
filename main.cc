#include <stdio.h>
#include <random>
#include <cstdint>
#include <unordered_map>
#include <cassert>
#include <string.h>

#include "buf.h"
#include "btree.h"
#include "rdtsc.h"

#define MAX_KEYS (1000000)
#define MAX_CHECK_KEY (10)


static double FREQ = 0;
static std::unordered_map<uint64_t, diskptr_t> keys;

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

class Stat {

public:
  Stat(std::string name) : name(name) {}

  void
  print_stat() {
    double average = total / num;
    average = cycles_to_us(average, FREQ);
    printf("[%s] Average: %f, Total: %f, Num: %f\n", 
      name.c_str(), average, total, num);
  }

  void add(uint64_t value) {
    total += value;
    num += 1;
  }

  std::string name;
  double total = 0;
  double num = 0;

};


void
general()
{
  keys = {};
  diskptr_t value;
  diskptr_t check;
  uint64_t start, stop;

  btree tree, oldtree;
  bool oldready = false;

  int error;

  diskptr_t ptr = allocate_blk(BLKSZ);

  printf("Calculating clock speed\n");
  FREQ = get_clock_speed_sleep();

  auto inserts = Stat("Inserts");
  auto deletes = Stat("Deletes");
  auto finds = Stat("Finds");
  auto checkpoints = Stat("Checkpoints");

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
    start = rdtscp();
    error = btree_insert(&tree, key, &value);
    stop = rdtscp();
    inserts.add(stop - start);
    assert(error == 0);

    /* lets check to see our old tree is still readable */
#ifdef CHECK_OLD_TREE
    if (oldready) {
      error = btree_find(&oldtree, key, &check);
      /* We should not find this latest key in our old tree */
      assert(error != 0);
    }
#endif

    start = rdtscp();
    error = btree_find(&tree, key, &check);
    stop = rdtscp();
    finds.add(stop - start);

    assert(error == 0);
    assert(memcmp(&check, &value, sizeof(diskptr_t)) == 0);

    int checkkey = 0;
    for (auto t : keys) {

      start = rdtscp();

      error = btree_find(&tree, t.first, &check);

      stop = rdtscp();
      finds.add(stop - start);

      assert(error == 0);
      assert(memcmp(&check, &t.second, sizeof(diskptr_t)) == 0);
      if (checkkey == MAX_CHECK_KEY) {
        break;
      }
      checkkey += 1;
    }

    /* Checkpoint but not on the empty tree */
    if ((i != 0) && ((i % 10000) == 0)) {
      start = rdtscp();
      btree_checkpoint(&tree);
      stop = rdtscp();
      checkpoints.add(stop - start);
      btree_init(&oldtree, tree.tr_ptr, sizeof(ptr));
      oldready = true;
    }
    if ((i % 100000) == 0) {
      printf("[Inserts Complete] %lu\n", i);
    }
  }

  printf("Done filling. Checking all keys\n");
  for (auto t : keys) {
    start = rdtscp();

    error = btree_find(&tree, t.first, &check);

    stop = rdtscp();
    finds.add(stop - start);

    assert(error == 0);
    assert(memcmp(&check, &t.second, sizeof(diskptr_t)) == 0);
  }

  printf("Deleting keys...\n");
  while (!keys.empty()) {
    auto it = keys.begin();

    start = rdtscp();

    btree_delete(&tree, it->first, &check);

    stop = rdtscp();

    deletes.add(stop - start);

    assert(memcmp(&check, &it->second, sizeof(diskptr_t)) == 0);
    bzero(&check, sizeof(diskptr_t));
    error = btree_find(&tree, it->first, &check);
    assert(error != 0);
    keys.erase(it); 

    int checkkey = 0;
    for (auto t : keys) {
      start = rdtscp();

      error = btree_find(&tree, t.first, &check);

      stop = rdtscp();
      finds.add(stop - start);

      assert(error == 0);
      assert(memcmp(&check, &t.second, sizeof(diskptr_t)) == 0);
      if (checkkey == MAX_CHECK_KEY) {
        break;
      }
      checkkey += 1;
    }
  }

  print_buf_stats();
  printf("Operation Stats in microseconds\n");
  inserts.print_stat();
  deletes.print_stat();
  finds.print_stat();
  checkpoints.print_stat();
}

kvp 
generate_kvp() {
  kvp ret;

  uint64_t key = generate_unique_key();
  while (keys.find(key) != keys.end()) {
    key = generate_unique_key();
  }
  diskptr_t value = generate_diskptr();
  keys.insert({key, value});
  ret.key = key;
  memcpy(&ret.data, &value, sizeof(diskptr_t));

  return ret;
}

#define BULK_KEYS_NUM (10000)

bool 
sort_by_key(const kvp &a, const kvp &b) {
  return a.key < b.key;
}

int 
bulkinsert() {
  keys = {};
  diskptr_t value;
  diskptr_t check;
  uint64_t start, stop;

  btree tree, oldtree;
  bool oldready = false;

  int error;

  diskptr_t ptr = allocate_blk(BLKSZ);
  printf("Calculating clock speed\n");
  FREQ = get_clock_speed_sleep();

  auto inserts = Stat("Inserts");
  auto bulkinserts = Stat("BulkInserts");
  auto deletes = Stat("Deletes");
  auto finds = Stat("Finds");
  auto checkpoints = Stat("Checkpoints");

  error = btree_init(&tree, ptr, sizeof(ptr));
  if (error) {
    printf("Problem initing\n");
  }
  std::vector<kvp> kvs;

  kvs.clear();
  for (int i = 0; i < BULK_KEYS_NUM; i++) {
    kvs.push_back(generate_kvp());
  }

  std::sort(kvs.begin(), kvs.end(), sort_by_key);

  printf("Bulk insert...\n");
  start = rdtscp();
  btree_bulkinsert(&tree, kvs.data(), kvs.size());
  stop = rdtscp();
  bulkinserts.add(stop - start);

  printf("Check key integrity\n");
  for (auto kv : kvs) {
    diskptr_t check;
    start = rdtscp();
    error = btree_find(&tree, kv.key, &check);
    stop = rdtscp();
    finds.add(stop - start);
    assert(error == 0);
    assert(memcmp(&check, &kv.data, sizeof(diskptr_t)) == 0);
  }


  kvs.clear();
  for (int i = 0; i < BULK_KEYS_NUM; i++) {
    kvs.push_back(generate_kvp());
  }

  printf("Bulk insert...\n");
  start = rdtscp();
  btree_bulkinsert(&tree, kvs.data(), kvs.size());
  stop = rdtscp();
  bulkinserts.add(stop - start);


  printf("Check key integrity\n");
  for (auto kv : kvs) {
    printf("%lu\n", kv.key);
    diskptr_t check;
    start = rdtscp();
    error = btree_find(&tree, kv.key, &check);
    stop = rdtscp();
    finds.add(stop - start);
    assert(error == 0);
    assert(memcmp(&check, &kv.data, sizeof(diskptr_t)) == 0);
  }

  ptr = allocate_blk(BLKSZ);
  error = btree_init(&tree, ptr, sizeof(ptr));
  if (error) {
    printf("Problem initing\n");
  }

  printf("Regular Insert...\n");
  for (auto kv : kvs) {
    diskptr_t check;
    start = rdtscp();
    error = btree_insert(&tree, kv.key, &kv.data);
    stop = rdtscp();
    inserts.add(stop - start);
    assert(error == 0);
  }

  for (auto kv : kvs) {
    diskptr_t check;
    start = rdtscp();
    error = btree_find(&tree, kv.key, &check);
    stop = rdtscp();
    finds.add(stop - start);
    assert(error == 0);
    assert(memcmp(&check, &kv.data, sizeof(diskptr_t)) == 0);
  }


  print_buf_stats();
  printf("Operation Stats in microseconds\n");
  inserts.print_stat();
  bulkinserts.print_stat();
  deletes.print_stat();
  finds.print_stat();
  checkpoints.print_stat();


  return 0;
}

int 
main(int argc, char *argv[]) {
  /* general(); */
  /* reset_buf_cache(); */
  bulkinsert();
  reset_buf_cache();

  return 0;
}
