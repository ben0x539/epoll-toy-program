#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

#include <signal.h>

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <netinet/in.h>
#include <netinet/in.h>

#ifndef DEBUG
   static void ignore_var_args(int i, ...);
#  define LOG(...) ignore_var_args(0, __VA_ARGS__)
#else
#  define LOG(...) fprintf(stderr, __VA_ARGS__)
#endif

typedef struct client_state {
  int fd; /* ought to be first member */
  unsigned rd_len;
  unsigned wr_offset;
  unsigned wr_len;
  uint32_t epoll_events;
  char rd_buf[512];
  char wr_buf[512*3];
} client_state;

typedef struct my_state {
  /* config bits */
  int desired_port;

  /* state bits */
  int accept_socket;
  int epoll_fd;
  int signal_fd;

  client_state** clients;
  size_t num_clients;
  size_t cap_clients;

  int shutdown;
} my_state;

static void maybe_close(int* fd);

static int run_server_loop(my_state* state, const char** err);
static int setup_accept_socket(int desired_port, const char** err);
static int setup_signal_fd(const char** err);

static void remove_client(my_state* state, client_state* client,
                          const char* msg);
static int send_to(my_state* state, client_state* client,
                   const char* msg, size_t len);
static int broadcast_from_rd(my_state* state, client_state* client,
                             const char* msg, size_t len);
static int try_write(my_state* state, client_state* client);

static void on_accept_cb(my_state* state);
static int on_signal_cb(my_state* state, const char** err);

static void on_hup_cb(my_state* state, client_state* client);
static void on_err_cb(my_state* state, client_state* client);
static void on_read_cb(my_state* state, client_state* client);
static void on_write_cb(my_state* state, client_state* client);

int main(int argc, char* argv[]) {
  const char* err;
  my_state state;
  int result;
  size_t i;

  state.desired_port = 8000;
  state.accept_socket = state.epoll_fd = state.signal_fd = -1;

  state.clients = NULL;
  state.num_clients = 0;
  state.cap_clients = 0;

  result = run_server_loop(&state, &err);
  maybe_close(&state.accept_socket);
  maybe_close(&state.epoll_fd);
  maybe_close(&state.signal_fd);

  for (i = 0; i < state.num_clients; ++i) {
    close(state.clients[i]->fd);
    free(state.clients[i]);
  }
  free(state.clients);

  if (result == -1) {
    perror(err);
    return -1;
  } else {
    return 0;
  }
}

static void maybe_close(int* fd) {
  if (*fd != -1)
    close(*fd);
  *fd = -1;
}

static int run_server_loop(my_state* state, const char** err) {
  struct epoll_event event;

  if ((state->accept_socket =
         setup_accept_socket(state->desired_port, err)) == -1)
    return -1;
  if ((state->signal_fd = setup_signal_fd(err)) == -1)
    return -1;
  if ((state->epoll_fd = epoll_create1(EPOLL_CLOEXEC)) == -1) {
    return -1;
  }

  signal(SIGPIPE, SIG_IGN);

  event.events = EPOLLIN;
  event.data.ptr = &state->accept_socket;
  if (epoll_ctl(state->epoll_fd, EPOLL_CTL_ADD,
                state->accept_socket, &event) == -1) {
    *err = "epoll_ctl (server)";
    return -1;
  }
  event.data.ptr = &state->signal_fd;
  if (epoll_ctl(state->epoll_fd, EPOLL_CTL_ADD,
                state->signal_fd, &event) == -1) {
    *err = "epoll_ctl (signal fd)";
    return -1;
  }

  state->shutdown = 0;
  while (state->shutdown == 0) {
    int result = epoll_wait(state->epoll_fd, &event, 1, -1);
    int fd;
    if (result == -1) {
      if (errno == EINTR)
        continue;

      *err = "epoll_wait";
      return -1;
    }

    assert(result == 1);
    assert(event.data.ptr != NULL);

    fd = *(int*) event.data.ptr; /* this is why client_state starts with fd */

    if (fd == state->accept_socket) {
      on_accept_cb(state);
    } else if (fd == state->signal_fd) {
      result = on_signal_cb(state, err);
    } else {
      /* it's a client fd, don't treat errors here, pass client state */
      client_state* client = (client_state*) event.data.ptr;
      if (event.events & (EPOLLRDHUP | EPOLLHUP)) {
        on_hup_cb(state, client);
      } else if (event.events & EPOLLERR) {
        on_err_cb(state, client);
      } else {
        if (event.events & EPOLLIN)
          on_read_cb(state, client);
        if (event.events & EPOLLOUT)
          on_write_cb(state, client);
      }
    }

    if (result == -1)
      return -1;
  }

  return 0;
}

static int setup_accept_socket(int desired_port, const char** err) {
  int fd;
  struct sockaddr_in addr;
  int one;
  int last_errno;

  fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd == -1) {
    *err = "socket (server)";
    return -1;
  }

  one = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one) == -1) {
    *err = "setsockopt (server)";
    goto close_and_fail;
  }

  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t) desired_port);
  addr.sin_addr.s_addr = INADDR_ANY;

  if (bind(fd, (struct sockaddr*) &addr, sizeof addr) == -1) {
    *err = "bind";
    goto close_and_fail;
  }

  if (listen(fd, 64) == -1) {
    *err = "listen";
    goto close_and_fail;
  }

  return fd;

close_and_fail:
  last_errno = errno;
  close(fd);
  errno = last_errno;
  return -1;
}

static int setup_signal_fd(const char** err) {
  int fd;
  sigset_t mask;

  if (sigemptyset(&mask) == -1) {
    *err = "sigemptyset";
    return -1;
  }

  if (sigaddset(&mask, SIGINT) == -1) {
    *err = "sigaddset";
    return -1;
  }

  if (sigprocmask(SIG_BLOCK, &mask, NULL) == -1) {
    *err = "sigprocmask";
    return -1;
  }

  fd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
  if (fd == -1) {
    *err = "signalfd";
    return -1;
  }

  return fd;
}

static void remove_client(my_state* state, client_state* client,
                          const char* msg) {
  size_t i;

  if (epoll_ctl(state->epoll_fd, EPOLL_CTL_DEL, client->fd, NULL) == -1) {
    perror("ignoring: epoll_ctl (removing client)");
  }

  if (msg) {
    /* "best-effort", no reasonable error checking */
    static const char prefix[] = "\ndisconnecting: ";
    write(client->fd, prefix, sizeof prefix);
    write(client->fd, msg, strlen(msg));
    write(client->fd, "\n", 1);
  }

  shutdown(client->fd, SHUT_RDWR);
  close(client->fd);

  /* gee, linear search :( */
  for (i = 0; i < state->num_clients; ++i)
    if (state->clients[i] == client)
      break;
  assert(i != state->num_clients);
  LOG("removing client fd %d, %d/%d\n", client->fd, (int) i, (int) state->num_clients);
  /* swap last entry to our position so we can remove our entry by
     decrementing the count. i guess nothing really goes wrong if
     we're already at the end. */
  state->clients[i] = state->clients[state->num_clients - 1];
  --state->num_clients;

  free(client);
}

static int send_to(my_state* state, client_state* client,
                   const char* msg, size_t len) {
  char err_buf[256];
  size_t n;
  size_t off;
  struct epoll_event event;

  if (client->wr_len + len + 1 > sizeof(client->wr_buf)) {
    remove_client(state, client, "write buffer overrun");
    return -1;
  }

  off = client->wr_offset + client->wr_len;
  if (off > sizeof(client->wr_buf))
    off -= sizeof(client->wr_buf);

  LOG("queueing for fd %d: %d += %d\n", client->fd, (int) client->wr_len, (int) len);

  n = sizeof(client->wr_buf) - off;
  if (n >= len + 1) {
    assert(off + len + 1 <= sizeof(client->wr_buf));
    memcpy(client->wr_buf + off, msg, len);
    client->wr_buf[off + len] = '\n';
  } else {
    assert(off + n <= sizeof(client->wr_buf));
    assert(len - n + 1 <= sizeof(client->wr_buf));
    memcpy(client->wr_buf + off, msg, n);
    memcpy(client->wr_buf, msg + n, len - n);
    client->wr_buf[len - n] = '\n';
  }
  client->wr_len += (unsigned) len + 1;

  if (client->wr_len >= 512) {
    /* one attempt at writing something already */
    if (try_write(state, client) == -1)
      return -1;
  }

  if (client->wr_len > 0 && !(client->epoll_events & EPOLLOUT)) {
    /* didn't get it all written */
    event.data.ptr = client;
    client->epoll_events = event.events = EPOLLIN | EPOLLRDHUP | EPOLLOUT;
    if (epoll_ctl(state->epoll_fd, EPOLL_CTL_MOD, client->fd, &event) == -1) {
      strerror_r(errno, err_buf, sizeof err_buf);
      remove_client(state, client, err_buf);
      return -1;
    }
  } else if (client->wr_len == 0 && (client->epoll_events & EPOLLOUT)) {
    /* did get everything written! */
    event.data.ptr = client;
    client->epoll_events = event.events = EPOLLIN | EPOLLRDHUP;
    if (epoll_ctl(state->epoll_fd, EPOLL_CTL_MOD, client->fd, &event) == -1) {
      strerror_r(errno, err_buf, sizeof err_buf);
      remove_client(state, client, err_buf);
      return -1;
    }
  }

  return 0;
}

static int try_write(my_state* state, client_state* client) {
  ssize_t result;
  char err_buf[256];
  size_t n;

  n = sizeof(client->wr_buf) - client->wr_offset;
  if (n > client->wr_len)
    n = client->wr_len;
  result = write(client->fd, client->wr_buf + client->wr_offset, n);
  if (result == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return 0;

    strerror_r(errno, err_buf, sizeof err_buf);
    remove_client(state, client, err_buf);
    return -1;
  }
  assert(result != 0);

  client->wr_len -= (unsigned) result;
  client->wr_offset += (unsigned) result;
  assert(client->wr_offset <= sizeof(client->wr_buf));
  if (client->wr_offset == sizeof(client->wr_buf))
      client->wr_offset = 0;

  return 1;
}

static int broadcast_from_rd(my_state* state, client_state* client,
                             const char* msg, size_t len) {
  size_t i;
  int result, client_result;
  char buf[sizeof(client->rd_buf)];
  client_state* receiver;

  memcpy(buf, msg, len);

  result = 0;
  for (i = 0; i < state->num_clients; ++i) {
    receiver = state->clients[i];
    client_result = send_to(state, receiver, buf, len);
    if (client_result == -1) {
      --i;
      if (receiver == client)
        result = -1;
    }
  }

  return result;
}

static void on_accept_cb(my_state* state) {
  int client_fd;
  int last_errno;
  struct sockaddr_in addr;
  socklen_t addr_len;
  int flags;
  client_state** clients;
  client_state* client;
  size_t cap;
  struct epoll_event event;
  const char* err;

  LOG("on_accept_cb\n");

  addr_len = sizeof addr;
  client_fd =
    accept(state->accept_socket, (struct sockaddr*) &addr, &addr_len);
  if (client_fd == -1) {
    err = "accept";
    goto close_and_fail;
  }
  flags = fcntl(client_fd, F_GETFL);
  if (flags == -1) {
    err = "fcntl (client_fd, F_GETFL)";
    goto close_and_fail;
  }
  if (fcntl(client_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    err = "fcntl (client_fd, F_SETFL, O_NONBLOCK)";
    goto close_and_fail;
  }
  flags = fcntl(client_fd, F_GETFD);
  if (flags == -1) {
    err = "fcntl (client_fd, F_GETFD)";
    goto close_and_fail;
  }
  if (fcntl(client_fd, F_SETFD, flags | FD_CLOEXEC) == -1) {
    err = "fcntl (client_fd, F_SETFD, FD_CLOEXEC)";
    goto close_and_fail;
  }

  cap = state->cap_clients;
  if (cap == state->num_clients * sizeof(*clients)) {
    if (cap == 0)
      cap = sizeof(*clients) * 4;
    else
      cap *= 2;
    assert(cap > state->cap_clients); /* overflow would be bad I guess */
    clients = realloc(state->clients, cap);
    if (clients == NULL) {
      err = "realloc (clients)";
      goto close_and_fail;
    }
    state->cap_clients = cap;
    state->clients = clients;
  }
  client = malloc(sizeof(client_state));
  if (client == NULL) {
    err = "malloc (client)";
    goto close_and_fail;
  }
  client->fd = client_fd;
  client->rd_len = 0;
  client->wr_offset = 0;
  client->wr_len = 0;
  state->clients[state->num_clients++] = client;

  event.data.ptr = client;
  client->epoll_events = event.events = EPOLLIN | EPOLLRDHUP;

  if (epoll_ctl(state->epoll_fd, EPOLL_CTL_ADD, client_fd, &event) == -1) {
    /* ugh */
    free(client);
    state->num_clients--;
    err = "epoll_ctl (adding client)";
    goto close_and_fail;
  }

  return;

close_and_fail:
  last_errno = errno;
  close(client_fd);
  errno = last_errno;

  fprintf(stderr, "ignoring: ");
  perror(err);
}

static int on_signal_cb(my_state* state, const char** err) {
  struct signalfd_siginfo info;
  ssize_t result;
  sigset_t mask;

  LOG("on_signal_cb\n");

  result = read(state->signal_fd, &info, sizeof info);
  if (result == -1) {
    *err = "read (signalfd)";
    return -1;
  }

  if (sigemptyset(&mask) == -1) {
    *err = "sigemptyset";
    return -1;
  }

  if (sigaddset(&mask, SIGINT) == -1) {
    *err = "sigaddset";
    return -1;
  }

  if (sigprocmask(SIG_UNBLOCK, &mask, NULL) == -1) {
    *err = "sigprocmask";
    return -1;
  }

  state->shutdown = -1;

  return 0;
}

static void on_hup_cb(my_state* state, client_state* client) {
  LOG("on_hup_cb\n");
  remove_client(state, client, NULL);
}

static void on_err_cb(my_state* state, client_state* client) {
  LOG("on_err_cb\n");
}

static void on_read_cb(my_state* state, client_state* client) {
  ssize_t result;
  unsigned len;
  char err_buf[256];
  char* start, * end, * p;
  LOG("on_read_cb\n");

  for (;;) {
    if (client->rd_len == sizeof(client->rd_buf)) {
      remove_client(state, client, "line too long");
      return;
    }

    result = read(client->fd, client->rd_buf + client->rd_len,
                  sizeof(client->rd_buf) - client->rd_len);
    if (result == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        return;
      strerror_r(errno, err_buf, sizeof err_buf);
      remove_client(state, client, err_buf);
      return;
    }
    if (result == 0) {
      remove_client(state, client, "eof");
      return;
    }

    assert(client->rd_len <= sizeof(client->rd_buf));
    start = client->rd_buf + client->rd_len;
    end = client->rd_buf + client->rd_len + result;
    for (;;) {
      p = memchr(start, '\n', (size_t) (end - start));
      if (!p)
        break;
      len = (unsigned) (p - start);
      LOG("received from fd %d: %.*s\n",
          client->fd,
          (int) len,
          start);
      if (broadcast_from_rd(state, client, start, len) == -1)
        return;
      start = p + 1;
    }
    client->rd_len = (unsigned) (end - start);
    memcpy(client->rd_buf, start, client->rd_len);
  }
}

static void on_write_cb(my_state* state, client_state* client) {
  char err_buf[256];
  struct epoll_event event;
  LOG("on_write_cb\n");

  while (client->wr_len > 0) {
    if (try_write(state, client) != 1)
      return;
  }

  client->wr_offset = 0;
  event.data.ptr = client;
  client->epoll_events = event.events = EPOLLIN | EPOLLRDHUP;

  if (epoll_ctl(state->epoll_fd, EPOLL_CTL_MOD, client->fd, &event) == -1) {
    strerror_r(errno, err_buf, sizeof err_buf);
    remove_client(state, client, err_buf);
    return;
  }
}

#ifndef DEBUG
  static void ignore_var_args(int i, ...) {}
#endif
