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
#include <dirent.h>

#define MAX_MESSAGE_LEN 1024
#define MAX_EVENTS 10

// Установка неблокирующего режима
void set_non_blocking(int sock) {
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
}

// Подсчет количества процессов через /proc
int get_process_count() {
    DIR *dir = opendir("/proc");
    if (!dir) return 0;

    int count = 0;
    struct dirent *entry;
    while ((entry = readdir(dir))) {
        // Проверяем, является ли имя каталога числом (PID процесса)
        char *endptr;
        strtol(entry->d_name, &endptr, 10);
        if (*endptr == '\0') {
            count++;
        }
    }
    closedir(dir);
    return count;
}

int main() {
    const char* SERVER_IP = getenv("SERVER_IP") ? getenv("SERVER_IP") : "127.0.0.1";
    int SERVER_PORT = getenv("SERVER_PORT") ? atoi(getenv("SERVER_PORT")) : 8080;
    
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

    // Асинхронное подключение
    int epoll_fd = epoll_create1(0);
    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLOUT;
    ev.data.fd = sock;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock, &ev);

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0 && errno != EINPROGRESS) {
        perror("Connection failed");
        return 1;
    }

    char send_buffer[MAX_MESSAGE_LEN * 10] = {0};
    size_t send_buffer_len = 0;
    time_t last_send = time(NULL);

    while (1) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == sock && (events[i].events & EPOLLOUT)) {
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

        // Формирование нового сообщения каждые 5 секунд
        if (time(NULL) - last_send >= 5) {
            int process_count = get_process_count();
            char message[MAX_MESSAGE_LEN];
            snprintf(message, MAX_MESSAGE_LEN, "PUBLISH system.processes Process Count: %d\n", process_count);
            size_t len = strlen(message);
            if (send_buffer_len + len < sizeof(send_buffer)) {
                memcpy(send_buffer + send_buffer_len, message, len);
                send_buffer_len += len;
                ev.events = EPOLLOUT;
                epoll_ctl(epoll_fd, EPOLL_CTL_MOD, sock, &ev);
                printf("Queued: %s", message);
            }
            last_send = time(NULL);
        }
    }

    close(sock);
    close(epoll_fd);
    return 0;
}
