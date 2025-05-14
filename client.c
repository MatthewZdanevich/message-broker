#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <errno.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 8080
#define MAX_MESSAGE_LEN 1024
#define BUFFER_SIZE 4096
#define MAX_EVENTS 10

// Установка неблокирующего режима
void set_non_blocking(int sock) {
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
}

// Функция для удаления \n
void remove_newline(char *str, size_t len) {
    for (size_t i = 0; i < len && str[i] != '\0'; i++) {
        if (str[i] == '\n') {
            str[i] = '\0';
            break;
        }
    }
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

    // Выбор темы
    printf("Available topics: system.cpu, system.memory, system.disk, system.network, system.load, system.processes\n");
    printf("Enter topic to subscribe: ");
    char topic[MAX_MESSAGE_LEN];
    fgets(topic, MAX_MESSAGE_LEN, stdin);
    remove_newline(topic, MAX_MESSAGE_LEN);

    char subscribe_cmd[MAX_MESSAGE_LEN];
    snprintf(subscribe_cmd, MAX_MESSAGE_LEN, "SUBSCRIBE %s\n", topic);
    char send_buffer[BUFFER_SIZE] = {0};
    size_t send_buffer_len = strlen(subscribe_cmd);
    memcpy(send_buffer, subscribe_cmd, send_buffer_len);

    char recv_buffer[BUFFER_SIZE] = {0};
    size_t recv_buffer_len = 0;

    while (1) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == sock) {
                if (events[i].events & EPOLLOUT) {
                    // Проверка состояния подключения
                    int error;
                    socklen_t len = sizeof(error);
                    getsockopt(sock, SOL_SOCKET, SO_ERROR, &error, &len);
                    if (error != 0) {
                        fprintf(stderr, "Connection failed: %s\n", strerror(error));
                        return 1;
                    }
                    // Отправка данных
                    if (send_buffer_len > 0) {
                        ssize_t sent = send(sock, send_buffer, send_buffer_len, 0);
                        if (sent > 0) {
                            memmove(send_buffer, send_buffer + sent, send_buffer_len - sent);
                            send_buffer_len -= sent;
                            if (send_buffer_len == 0) {
                                ev.events = EPOLLIN;
                                epoll_ctl(epoll_fd, EPOLL_CTL_MOD, sock, &ev);
                            }
                        }
                    }
                }
                if (events[i].events & EPOLLIN) {
                    // Прием данных
                    char buffer[BUFFER_SIZE];
                    ssize_t bytes_read = read(sock, buffer, sizeof(buffer) - 1);
                    if (bytes_read <= 0) {
                        printf("Disconnected from broker\n");
                        close(sock);
                        close(epoll_fd);
                        return 0;
                    }
                    for (ssize_t j = 0; j < bytes_read; j++) {
                        if (buffer[j] == '\n') {
                            recv_buffer[recv_buffer_len] = '\0';
                            if (recv_buffer_len > 0) {
                                printf("Received: %s\n", recv_buffer);
                            }
                            recv_buffer_len = 0;
                        } else {
                            if (recv_buffer_len < BUFFER_SIZE - 1) {
                                recv_buffer[recv_buffer_len++] = buffer[j];
                            }
                        }
                    }
                }
            }
        }
    }

    close(sock);
    close(epoll_fd);
    return 0;
}
