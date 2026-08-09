// business_demo_heap.cpp — тот же сценарий, что и business_demo.cpp
// (бизнес-логика на файбере + отдельный писательский поток + очереди),
// но поверх storage.hpp вместо record.hpp: storage::storage<user_spec> вместо
// storage::engine<user_spec> + eng.of<user>(). Разница по существу:
//   - нет отдельного eng.of<user>() — storage САМ и есть таблица.
//   - user не хранит свой id внутри payload'а — индекс уже знает id,
//     дублировать его в блобе незачем (см. обсуждение).
//   - у user появился tag (нужен tagged_aggregate concept), назначается
//     вручную через enum kind.
//
// Собирать: g++ -std=c++26 -O2 -pthread business_demo_heap.cpp -o business_demo_heap

#include "../runtime/fiber.hpp"
#include "../storage/storage.hpp"
#include "../runtime/mt_queue.hpp"

#include <cstdio>
#include <thread>
#include <atomic>
#include <unordered_map>

enum class kind : storage::tag_t { user = 1 };

struct user {
    static constexpr storage::tag_t tag = (storage::tag_t)kind::user;
    uint64_t balance;
};

using user_storage_spec = storage::storage_spec<storage::durability::local_sync, "users_demo.db">;

struct write_req { uint64_t req_id; storage::record_id user_id; uint64_t new_balance; };
struct write_ack { uint64_t req_id; };

mt::queue<write_req> requests;
mt::queue<write_ack> acks;
std::atomic<bool> running{true};

// --- писательский поток: единственный, кто трогает table после старта
// (storage<spec> строго однопоточный). ---
void writer_thread(storage::storage<user_storage_spec>& table) {
    while (running.load()) {
        auto r = requests.pop_blocking();
        if (!r) continue; // разбужены stop()'ом
        table.update<&user::balance>(r->user_id, r->new_balance);
        table.sync(); // реальный msync() — здесь, не в потоке с файберами
        acks.push(write_ack{ r->req_id });
    }
}

// --- бизнес-логика на файбере: без изменений по сравнению с оригиналом ---
std::unordered_map<uint64_t, bool> completed;
uint64_t next_req_id = 1;

uint64_t submit(storage::record_id user_id, uint64_t new_balance) {
    uint64_t id = next_req_id++;
    requests.push(write_req{ id, user_id, new_balance });
    return id;
}

void await(uint64_t req_id) {
    while (!completed.contains(req_id)) fiber::yield();
    completed.erase(req_id);
}

void transfer(storage::record_id from_id, uint64_t current_from,
              storage::record_id to_id, uint64_t current_to, uint64_t amount) {
    printf("[transfer] списываем %llu с user#%llu\n", (unsigned long long)amount, (unsigned long long)from_id);
    auto req1 = submit(from_id, current_from - amount);
    await(req1);

    printf("[transfer] зачисляем %llu на user#%llu\n", (unsigned long long)amount, (unsigned long long)to_id);
    auto req2 = submit(to_id, current_to + amount);
    await(req2);

    printf("[transfer] готово\n");
}

int main() {
    std::remove("users_demo.db");

    // local_sync — storage сам открывает и маппит файл, shared_base не нужен
    // (в отличие от старого engine, которому он был нужен ВСЕГДА, даже
    // без единой shared/replicated таблицы).
    storage::storage<user_storage_spec> table;

    auto alice = table.create(user{ 1000 });
    auto bob   = table.create(user{ 500 });
    table.sync();

    std::thread writer(writer_thread, std::ref(table));

    fiber::scheduler sched;
    sched.spawn([&] { transfer(alice, 1000, bob, 500, /*amount=*/300); });
    sched.spawn([&] {
        for (int i = 0; i < 3; ++i) {
            printf("[background] тик %d, пока transfer ждёт диск\n", i);
            fiber::yield();
        }
    });

    while (sched.has_pending()) {
        while (auto a = acks.pop()) completed[a->req_id] = true;
        sched.run_once();
    }

    running = false;
    requests.stop();
    writer.join(); // после join() снова безопасно читать table с главного потока

    const user* a = table.get<user>(alice);
    const user* b = table.get<user>(bob);
    printf("\nитог: alice=%llu bob=%llu\n", (unsigned long long)a->balance, (unsigned long long)b->balance);
    return (a->balance == 700 && b->balance == 800) ? 0 : 1;
}
