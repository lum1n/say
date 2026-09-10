#include <td/telegram/td_json_client.h>

#include <atomic>
#include <iostream>
#include <string>
#include <thread>

int main() {
  auto *client = td_json_client_create();
  if (client == nullptr) {
    std::cerr << "failed to create TDLib JSON client\n";
    return 1;
  }

  std::atomic<bool> running{true};
  std::thread receiver([&]() {
    while (running.load()) {
      if (const char *result = td_json_client_receive(client, 1.0)) {
        std::cout << result << '\n' << std::flush;
      }
    }
  });

  std::string request;
  while (std::getline(std::cin, request)) {
    if (!request.empty()) td_json_client_send(client, request.c_str());
  }

  running.store(false);
  receiver.join();
  td_json_client_destroy(client);
  return 0;
}
