#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <time.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/sysinfo.h>
#include <sys/timerfd.h>
#include <inttypes.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 8080
#define MAX_MESSAGE_LEN 1024
#define MAX_EVENTS 10

void set_non_blocking(int sock) {
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
}

int count_cpu_cores() {
    FILE *file = fopen("/proc/cpuinfo", "r");
    if (!file) return -1;

    int cores = 0;
    char line[128];
    while (fgets(line, sizeof(line), file) {
        if (strncmp(line, "processor", 9) == 0) {
            cores++;
        }
    }
    fclose(file);
    return cores;
}

double get_cpu_usage() {
    struct sysinfo info;
    if (sysinfo(&info) != 0) {
        perror("sysinfo");
        return 1;
    }

    return (((double)info.loads[0] / (1 << SI_LOAD_SHIFT)) / count_cpu_cores()) * 100;
}

int main() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        return 1;
    }
    set_non_blocking(sock);

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr);

    // Создаем epoll
    int epoll_fd = epoll_create1(0);
    struct epoll_event ev, events[MAX_EVENTS];

    // Добавляем сокет в epoll для отслеживания подключения
    ev.events = EPOLLOUT;
    ev.data.fd = sock;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock, &ev);

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0 && errno != EINPROGRESS) {
        perror("Connection failed");
        return 1;
    }

    // Создаем таймер
    int timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timer_fd == -1) {
        perror("timerfd_create");
        return 1;
    }

    // Настраиваем таймер на срабатывание каждые 5 секунд
    struct itimerspec timer_spec = {
        .it_interval = {.tv_sec = 5, .tv_nsec = 0},  // интервал
        .it_value = {.tv_sec = 5, .tv_nsec = 0}      // первое срабатывание
    };
    if (timerfd_settime(timer_fd, 0, &timer_spec, NULL) == -1) {
        perror("timerfd_settime");
        close(timer_fd);
        return 1;
    }

    // Добавляем таймер в epoll
    ev.events = EPOLLIN;
    ev.data.fd = timer_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, timer_fd, &ev) == -1) {
        perror("epoll_ctl: timer_fd");
        close(timer_fd);
        return 1;
    }

    char send_buffer[MAX_MESSAGE_LEN * 10] = {0};
    size_t send_buffer_len = 0;

    while (1) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == timer_fd && (events[i].events & EPOLLIN)) {
                // Обработка срабатывания таймера
                uint64_t expirations;
                read(timer_fd, &expirations, sizeof(expirations));

                // Формирование нового сообщения
                double cpu_usage = get_cpu_usage();
                char message[MAX_MESSAGE_LEN];
                snprintf(message, MAX_MESSAGE_LEN, "PUBLISH system.cpu CPU Usage: %.2f%%\n", cpu_usage);
                size_t len = strlen(message);
                if (send_buffer_len + len < sizeof(send_buffer)) {
                    memcpy(send_buffer + send_buffer_len, message, len);
                    send_buffer_len += len;
                    ev.events = EPOLLOUT;
                    ev.data.fd = sock;
                    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, sock, &ev);
                    printf("Queued: %s", message);
                }
            }
            else if (events[i].data.fd == sock && (events[i].events & EPOLLOUT)) {
                // Проверка состояния подключения
                int error;
                socklen_t len = sizeof(error);
                getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, &len);
                if (error != 0) {
                    fprintf(stderr, "Connection failed: %s\n", strerror(error));
                    return 1;
                }
                // Отправка данных из буфера
                if (send_buffer_len > 0) {
                    ssize_t sent = send(sock, send_buffer, send_buffer_len, 0);
                    if (sent > 0) {
                        memmove(send_buffer, send_buffer + sent, send_buffer_len - sent);
                        send_buffer_len -= sent;
                        if (send_buffer_len == 0) {
                            ev.events = 0; // Отключаем EPOLLOUT
                            epoll_ctl(epoll_fd, EPOLL_CTL_MOD, sock, &ev);
                        }
                    }
                }
            }
        }
    }

    close(timer_fd);
    close(sock);
    close(epoll_fd);
    return 0;
}