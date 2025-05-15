#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <pthread.h>

#define MAX_SUBSCRIBERS 100
#define MAX_MESSAGES_PER_TOPIC 100
#define MAX_MESSAGE_LEN 1024
#define MAX_EVENTS 10
#define MAX_TOPIC_LEN 256
#define QUEUE_SIZE 1000

// Структура для сообщения
typedef struct {
    char data[MAX_MESSAGE_LEN];
    char topic[MAX_TOPIC_LEN];
} Message;

// Структура для очереди сообщений
typedef struct {
    Message messages[QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} MessageQueue;

// Структура для темы
typedef struct Topic {
    char name[MAX_TOPIC_LEN];
    Message messages[MAX_MESSAGES_PER_TOPIC];
    int message_count;
    struct Topic *next;
} Topic;

// Структура для подписчика
typedef struct {
    int socket;
    char topic[MAX_TOPIC_LEN];
    char send_buffer[MAX_MESSAGE_LEN * 10]; // Буфер для исходящих сообщений
    size_t send_buffer_len;
} Subscriber;

Topic *topics = NULL;
Subscriber subscribers[MAX_SUBSCRIBERS];
int subscriber_count = 0;
MessageQueue message_queue;
int epoll_fd;

// Инициализация очереди сообщений
void init_message_queue() {
    message_queue.head = 0;
    message_queue.tail = 0;
    message_queue.count = 0;
    pthread_mutex_init(&message_queue.mutex, NULL);
    pthread_cond_init(&message_queue.cond, NULL);
}

// Добавление сообщения в очередь
int enqueue_message(const char *topic, const char *data) {
    pthread_mutex_lock(&message_queue.mutex);
    if (message_queue.count >= QUEUE_SIZE) {
        pthread_mutex_unlock(&message_queue.mutex);
        return -1; // Очередь полна
    }
    strncpy(message_queue.messages[message_queue.tail].topic, topic, MAX_TOPIC_LEN - 1);
    strncpy(message_queue.messages[message_queue.tail].data, data, MAX_MESSAGE_LEN - 1);
    message_queue.tail = (message_queue.tail + 1) % QUEUE_SIZE;
    message_queue.count++;
    pthread_cond_signal(&message_queue.cond);
    pthread_mutex_unlock(&message_queue.mutex);
    return 0;
}

// Извлечение сообщения из очереди
int dequeue_message(Message *msg) {
    pthread_mutex_lock(&message_queue.mutex);
    while (message_queue.count == 0) {
        pthread_cond_wait(&message_queue.cond, &message_queue.mutex);
    }
    *msg = message_queue.messages[message_queue.head];
    message_queue.head = (message_queue.head + 1) % QUEUE_SIZE;
    message_queue.count--;
    pthread_mutex_unlock(&message_queue.mutex);
    return 0;
}

// Установка неблокирующего режима для сокета
void set_non_blocking(int sock) {
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
}

// Функция для удаления \n из строки
void remove_newline(char *str, size_t len) {
    for (size_t i = 0; i < len && str[i] != '\0'; i++) {
        if (str[i] == '\n') {
            str[i] = '\0';
            break;
        }
    }
}

// Поиск или создание темы
Topic* find_or_create_topic(const char *name) {
    Topic *current = topics;
    while (current) {
        if (strcmp(current->name, name) == 0) {
            return current;
        }
        current = current->next;
    }
    Topic *new_topic = (Topic*)malloc(sizeof(Topic));
    strncpy(new_topic->name, name, MAX_TOPIC_LEN - 1);
    new_topic->message_count = 0;
    new_topic->next = topics;
    topics = new_topic;
    return new_topic;
}

// Сохранение сообщения
void store_message(const char *topic_name, const char *message) {
    Topic *topic = find_or_create_topic(topic_name);
    if (topic->message_count < MAX_MESSAGES_PER_TOPIC) {
        char clean_message[MAX_MESSAGE_LEN];
        strncpy(clean_message, message, MAX_MESSAGE_LEN - 1);
        clean_message[strcspn(clean_message, "\n")] = '\0';
        strncpy(topic->messages[topic->message_count].data, clean_message, MAX_MESSAGE_LEN - 1);
        topic->message_count++;
    }
}

// Поток для обработки очереди сообщений
void* message_processor(void *arg) {
    Message msg;
    while (1) {
        dequeue_message(&msg);
        store_message(msg.topic, msg.data);
        char send_buffer[MAX_TOPIC_LEN + MAX_MESSAGE_LEN + 10]; // +1 для \n
        snprintf(send_buffer, MAX_MESSAGE_LEN + 1, "PUBLISH %s %s\n", msg.topic, msg.data);
        for (int i = 0; i < subscriber_count; i++) {
            if (strcmp(subscribers[i].topic, msg.topic) == 0) {
                size_t len = strlen(send_buffer);
                if (subscribers[i].send_buffer_len + len < sizeof(subscribers[i].send_buffer)) {
                    memcpy(subscribers[i].send_buffer + subscribers[i].send_buffer_len, send_buffer, len);
                    subscribers[i].send_buffer_len += len;
                    // Добавляем сокет в epoll для записи
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLOUT;
                    ev.data.fd = subscribers[i].socket;
                    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, subscribers[i].socket, &ev);
                }
            }
        }
    }
    return NULL;
}

int main() {
    int server_fd;
    struct sockaddr_in server_addr;
    struct epoll_event ev, events[MAX_EVENTS];

    init_message_queue();

    // Создаём поток для обработки очереди
    pthread_t processor_thread;
    pthread_create(&processor_thread, NULL, message_processor, NULL);

    // Создаём сокет
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(8080);

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr));
    listen(server_fd, 10);
    set_non_blocking(server_fd);

    // Создаём epoll
    epoll_fd = epoll_create1(0);
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    printf("Broker running...\n");

    while (1) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == server_fd) {
                // Новое соединение
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_socket = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
                set_non_blocking(client_socket);
                ev.events = EPOLLIN;
                ev.data.fd = client_socket;
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_socket, &ev);
                printf("New client connected: %d\n", client_socket);
            } else {
                int client_socket = events[i].data.fd;
                if (events[i].events & EPOLLIN) {
                    // Данные от клиента
                    char buffer[MAX_MESSAGE_LEN];
                    int bytes_read = read(client_socket, buffer, sizeof(buffer) - 1);
                    if (bytes_read <= 0) {
                        // Клиент отключился
                        printf("Client disconnected: %d\n", client_socket);
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_socket, NULL);
                        for (int j = 0; j < subscriber_count; j++) {
                            if (subscribers[j].socket == client_socket) {
                                subscribers[j] = subscribers[--subscriber_count];
                                break;
                            }
                        }
                        close(client_socket);
                        continue;
                    }
                    buffer[bytes_read] = '\0';
                    remove_newline(buffer, bytes_read);

                    if (strncmp(buffer, "SUBSCRIBE ", 10) == 0) {
                        // Регистрируем подписку
                        char *topic = buffer + 10;
                        int already_subscribed = 0;
                        for (int j = 0; j < subscriber_count; j++) {
                            if (subscribers[j].socket == client_socket) {
                                already_subscribed = 1;
                                break;
                            }
                        }
                        if (!already_subscribed && subscriber_count < MAX_SUBSCRIBERS) {
                            subscribers[subscriber_count].socket = client_socket;
                            subscribers[subscriber_count].send_buffer_len = 0;
                            strncpy(subscribers[subscriber_count].topic, topic, MAX_TOPIC_LEN - 1);
                            subscriber_count++;
                            char *response = "Subscribed\n";
                            send(client_socket, response, strlen(response), 0);
                            printf("Client %d subscribed to %s\n", client_socket, topic);

                            // Отправляем сохранённые сообщения
                            Topic *topic_ptr = find_or_create_topic(topic);
                            for (int j = 0; j < topic_ptr->message_count; j++) {
                                char send_buffer[MAX_MESSAGE_LEN + 1]; // +1 для \n
                                snprintf(send_buffer, MAX_MESSAGE_LEN + 1, "%s\n", topic_ptr->messages[j].data);
                                size_t len = strlen(send_buffer);
                                if (subscribers[subscriber_count - 1].send_buffer_len + len < sizeof(subscribers[subscriber_count - 1].send_buffer)) {
                                    memcpy(subscribers[subscriber_count - 1].send_buffer + subscribers[subscriber_count - 1].send_buffer_len, send_buffer, len);
                                    subscribers[subscriber_count - 1].send_buffer_len += len;
                                    ev.events = EPOLLIN | EPOLLOUT;
                                    ev.data.fd = client_socket;
                                    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_socket, &ev);
                                }
                            }
                        } else {
                            send(client_socket, "Already subscribed or too many subscribers\n", 42, 0);
                        }
                    } else if (strncmp(buffer, "PUBLISH ", 8) == 0) {
                        // Публикуем сообщение асинхронно
                        char *topic = strtok(buffer + 8, " ");
                        char *message = strtok(NULL, "");
                        if (topic && message) {
                            enqueue_message(topic, message);
                            printf("Queued message for %s: %s\n", topic, message);
                        }
                    } else {
                        send(client_socket, "Unknown command\n", 16, 0);
                    }
                }
                if (events[i].events & EPOLLOUT) {
                    // Отправка данных из буфера
                    for (int j = 0; j < subscriber_count; j++) {
                        if (subscribers[j].socket == client_socket && subscribers[j].send_buffer_len > 0) {
                            ssize_t sent = send(client_socket, subscribers[j].send_buffer, subscribers[j].send_buffer_len, 0);
                            if (sent > 0) {
                                memmove(subscribers[j].send_buffer, subscribers[j].send_buffer + sent, subscribers[j].send_buffer_len - sent);
                                subscribers[j].send_buffer_len -= sent;
                                if (subscribers[j].send_buffer_len == 0) {
                                    // Убираем EPOLLOUT, если буфер пуст
                                    struct epoll_event ev;
                                    ev.events = EPOLLIN;
                                    ev.data.fd = client_socket;
                                    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_socket, &ev);
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    // Очистка
    Topic *current = topics;
    while (current) {
        Topic *next = current->next;
        free(current);
        current = next;
    }
    pthread_mutex_destroy(&message_queue.mutex);
    pthread_cond_destroy(&message_queue.cond);
    close(server_fd);
    close(epoll_fd);
    return 0;
}
