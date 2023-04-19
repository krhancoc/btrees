#include <stdio.h>
#include <random>
#include <cstdint>
#include <unordered_map>
#include <cassert>
#include <string.h>

#include "buf.h"
#include "btree.h"
#include "rdtsc.h"

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
#define MAX_CHECK_KEY (10)

double total_time_insert = 0;
double num_inserts = 0;

double total_time_find = 0;
double num_finds = 0;

double total_time_delete = 0;
double num_delete = 0;

static void 
print_stat(std::string val, double total_time, double num) {
  double average = total_time / num;
  printf("[%s] Average: %f, Total: %f, Num: %f\n", 
      val.c_str(), average, total_time, num);
}

int main(int argc, char *argv[])
{
  std::unordered_map<uint64_t, diskptr_t> keys;
  diskptr_t value;
  diskptr_t check;
  uint64_t start, stop;
  btree tree;
  int error;
  diskptr_t ptr = allocate_blk(BLKSZ);
  printf("Calculating clock speed\n");
  auto clkspeed = get_clock_speed_sleep();


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
    btree_insert(&tree, key, &value);
    stop = rdtscp();
    total_time_insert += stop - start;
    num_inserts += 1;

    start = rdtscp();
    error = btree_find(&tree, key, &check);
    stop = rdtscp();
    total_time_find += stop - start;
    num_finds += 1;

    assert(error == 0);
    assert(memcmp(&check, &value, sizeof(diskptr_t)) == 0);

    int checkkey = 0;
    for (auto t : keys) {

      start = rdtscp();

      error = btree_find(&tree, t.first, &check);

      stop = rdtscp();
      total_time_find += stop - start;
      num_finds += 1;

      assert(error == 0);
      assert(memcmp(&check, &t.second, sizeof(diskptr_t)) == 0);
      if (checkkey == MAX_CHECK_KEY) {
        break;
      }
      checkkey += 1;
    }
  }

  for (auto t : keys) {
    start = rdtscp();

    error = btree_find(&tree, t.first, &check);

    stop = rdtscp();
    total_time_find += stop - start;
    num_finds += 1;

    assert(error == 0);
    assert(memcmp(&check, &t.second, sizeof(diskptr_t)) == 0);
  }

  printf("Deleting keys...\n");
  while (!keys.empty()) {
    auto it = keys.begin();

    start = rdtscp();

    btree_delete(&tree, it->first, &check);

    stop = rdtscp();
    total_time_delete += stop - start;
    num_delete += 1;

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
      total_time_find += stop - start;
      num_finds += 1;

      assert(error == 0);
      assert(memcmp(&check, &t.second, sizeof(diskptr_t)) == 0);
      if (checkkey == MAX_CHECK_KEY) {
        break;
      }
      checkkey += 1;
    }
  }

  print_buf_stats();
  printf("Operation Stats in Cycles\n");
  print_stat("Inserts", total_time_insert, num_inserts);
  print_stat("Deletes", total_time_delete, num_delete);
  print_stat("Find", total_time_find, num_finds);

  return 0;
}
