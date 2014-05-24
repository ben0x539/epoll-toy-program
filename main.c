#define _POSIX_SOURCE

#include <errno.h>
#include <stdio.h>
#include <assert.h>

#include <signal.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <netinet/in.h>
#include <netinet/in.h>

typedef struct my_state {
  /* config bits */
  int desired_port;

  /* state bits */
  int accept_socket;
  int epoll_fd;
  int signal_fd;

  int shutdown;
} my_state;

typedef int (*my_epoll_cb)(my_state* state, const char** err);

static void maybe_close(int* fd);

static int run_server_loop(my_state* state, const char** err);
static int setup_accept_socket(int desired_port, const char** err);
static int setup_signal_fd(const char** err);

static int on_accept_cb(my_state* state, const char** err);
static int on_signal_cb(my_state* state, const char** err);

static my_epoll_cb on_accept_cb_ptr = &on_accept_cb;
static my_epoll_cb on_signal_cb_ptr = &on_signal_cb;

int main(int argc, char* argv[]) {
  const char* err;
  my_state state;
  int result;

  state.desired_port = 8000;
  state.accept_socket = state.epoll_fd = state.signal_fd = -1;

  result = run_server_loop(&state, &err);
  maybe_close(&state.accept_socket);
  maybe_close(&state.epoll_fd);
  maybe_close(&state.signal_fd);

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

  event.events = EPOLLIN;
  event.data.ptr = &on_accept_cb_ptr;
  if (epoll_ctl(state->epoll_fd, EPOLL_CTL_ADD,
                state->accept_socket, &event) == -1) {
    *err = "epoll_ctl (server)";
    return -1;
  }
  event.data.ptr = &on_signal_cb_ptr;
  if (epoll_ctl(state->epoll_fd, EPOLL_CTL_ADD,
                state->signal_fd, &event) == -1) {
    *err = "epoll_ctl (signal fd)";
    return -1;
  }

  state->shutdown = 0;
  while (state->shutdown == 0) {
    int result = epoll_wait(state->epoll_fd, &event, 1, -1);
    if (result == -1) {
      if (errno == EINTR)
        continue;

      *err = "epoll_wait";
      return -1;
    }

    assert(result == 1);

    result = (**(my_epoll_cb*) (event.data.ptr))(state, err);

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

static int on_accept_cb(my_state* state, const char** err) {
  int client_fd;
  struct sockaddr_in addr;
  socklen_t addr_len;

  printf("on_accept_cb\n");

  addr_len = sizeof addr;
  client_fd =
    accept(state->accept_socket, (struct sockaddr*) &addr, &addr_len);
  if (client_fd == -1) {
    *err = "accept";
    return -1;
  }

  return 0;
}

static int on_signal_cb(my_state* state, const char** err) {
  struct signalfd_siginfo info;
  ssize_t result;
  sigset_t mask;

  printf("on_signal_cb\n");

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
