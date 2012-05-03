/**
 * @file multi.c
 * @author Ohtaman
 * @brief
 *
 * @date Thu May  3 22:13:41 2012 last updated
 * @date Fri Apr 20 04:53:10 2012 created
 */

#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <errno.h>

#include "config.h"

#ifndef TRUE
#define TRUE (1)
#endif
#ifndef FALSE
#define FALSE (0)
#endif

static char *tmpdir_name = NULL;
static char **in_fifos = NULL;
static char **out_fifos = NULL;
int parent_proc = TRUE;
int DEFAULT_BUFF_SIZE = 1024;

typedef struct {
  int num_mapper;
  int sequential;
  char *splitter;
  char *mapper;
  char *combiner;
} opts_t;

typedef struct {
  int in_dsc;
  int out_dsc;
  pthread_mutex_t *mutex;
} thread_arg_t;

opts_t *create_opts(int argc, char **argv);
void show_help();
void clear_opts(opts_t *opts);
void exec_splitter(char *cmd, int in, char** outs, int num);
void exec_mapper(char *cmd, int in_dsc, int out_dsc);
void exec_combiner(char *cmd, char **ins, int out_dsc, int num);
char *create_tmpdir();
void cleanup_tmpdir();
int is_valid_dir(char *path);
void split_default(int in_dsc, char** outs, int num);
void split_sequential(int in_dsc, char** outs, int num);
void combine_default(char** ins, int out_dsc, int num);
void combine_sequential(char** ins, int out_dsc, int num);
void *pomp(void *args);
int strrep(char *dest, char *src, char *before, char *after);
int strjoin(char *dest, char **src, int num, char *delim);
void wait_all();
void trap_int();

int main(int argc, char **argv)
{
  int i;
  char *tmpdir;
  pid_t splitter_pid;
  pid_t *mapper_pid;
  pid_t combiner_pid;
  opts_t *opts;
  opts = create_opts(argc, argv);
  if(!opts) {
    exit(-1);
  }

  signal(SIGINT, trap_int);
  signal(SIGKILL, trap_int);

  tmpdir = create_tmpdir();
  if (tmpdir == NULL) {
    exit(-1);
  }

  in_fifos = calloc(opts->num_mapper + 1, sizeof(size_t));
  for (i = 0; i < opts->num_mapper; ++i) {
    char *name;
    name = malloc(sizeof(char)*(strlen(tmpdir) + 11));
    sprintf(name, "%s/in_%d", tmpdir, i);
    if(mkfifo(name, 0600) == 0) {
      in_fifos[i] = name;
    } else {
      free(name);
    }
  }

  out_fifos = calloc(opts->num_mapper + 1, sizeof(size_t));
  for (i = 0; i < opts->num_mapper; ++i) {
    char *name;
    name = malloc(sizeof(char)*(strlen(tmpdir) + 11));
    sprintf(name, "%s/out_%d", tmpdir, i);
    if(mkfifo(name, 0600) == 0) {
      out_fifos[i] = name;
    } else {
      free(name);
    }
  }

  splitter_pid = fork();
  if (splitter_pid < 0) {
    exit(-1);
  }

  if(!splitter_pid) {
    parent_proc = FALSE;
    if (opts->sequential) {
      split_sequential(0, in_fifos, opts->num_mapper);
    } else if (opts->splitter) {
      exec_splitter(opts->splitter, 0, in_fifos, opts->num_mapper);
    } else {
      split_default(0, in_fifos, opts->num_mapper);
    }
    exit(0);
  }

  mapper_pid = calloc(opts->num_mapper, sizeof(pid_t));
  for (i = 0; i < opts->num_mapper; ++i) {
    mapper_pid[i] = fork();
    if (mapper_pid[i] < 0) {
      wait_all();
      exit(-1);
    }
    if (!mapper_pid[i]) {
      int in_dsc;
      int out_dsc;
      parent_proc = FALSE;
      in_dsc = open(in_fifos[i], O_RDONLY);
      out_dsc = open(out_fifos[i], O_WRONLY);
      exec_mapper(opts->mapper, in_dsc, out_dsc);
      close(in_dsc);
      close(out_dsc);
      exit(0);
    }
  }


  combiner_pid = fork();
  if (combiner_pid < 0) {
    wait_all();
    exit(-1);
  }

  if (!combiner_pid) {
    parent_proc = FALSE;
    if (opts->sequential) {
      combine_sequential(out_fifos, 1, opts->num_mapper);
    } else if (opts->combiner) {
      exec_combiner(opts->combiner, out_fifos, 1, opts->num_mapper);
    } else {
      combine_default(out_fifos, 1, opts->num_mapper);
    }
    exit(0);
  }

  wait_all();
  cleanup_tmpdir();
  clear_opts(opts);
  return 0;
}

opts_t *create_opts(int argc, char **argv)
{
  int opt;
  opts_t *opts;
  opts = calloc(1, sizeof(opts_t));
  if (!opts) {
    return NULL;
  }

  while ((opt = getopt(argc, argv, "n:s:m:c:S")) != EOF) {
    switch (opt) {
    case 'n':
      opts->num_mapper = atoi(optarg);
      break;
    case 's':
      opts->splitter = malloc(sizeof(char)*(strlen(optarg) + 1));
      strcpy(opts->splitter, optarg);
      break;
    case 'm':
      opts->mapper = malloc(sizeof(char)*(strlen(optarg) + 1));
      strcpy(opts->mapper, optarg);
      break;
    case 'c':
      opts->combiner = malloc(sizeof(char)*(strlen(optarg) + 1));
      strcpy(opts->combiner, optarg);
      break;
    case 'S':
      opts->sequential = TRUE;
      break;
    default:
      clear_opts(opts);
      return NULL;
    }
  }

  if (opts->num_mapper <= 0) {
    opts->num_mapper = 1;
  }

  if (!opts->mapper) {
    clear_opts(opts);
    return NULL;
  }

  return opts;
}

void clear_opts(opts_t *opts)
{
  if (opts) {
    if (opts->splitter) {
      free(opts->splitter);
    }
    if (opts->mapper) {
      free(opts->mapper);
    }
    if (opts->combiner) {
      free(opts->combiner);
    }
    free(opts);
  }
}

char *create_tmpdir()
{
  char *prefix;
  char *dir_name;
  if (tmpdir_name == NULL) {
    char *parent;
    parent = getenv("TMPDIR");

    if (!is_valid_dir(parent)) {
      parent = P_tmpdir;
      if (!is_valid_dir(parent)) {
        return NULL;
      }
    }

    prefix = "multi-";
    dir_name = malloc(sizeof(char)*(strlen(parent) + strlen(prefix) + 12));
    if (dir_name) {
      int i;
      i = 0;
      do {
        sprintf(dir_name, "%s/%s%d", parent, prefix, i);
        ++i;
      } while (mkdir(dir_name, 0700) == -1);
      tmpdir_name = dir_name;
    }
  }

  return tmpdir_name;
}

int is_valid_dir(char *path)
{
  return path != NULL && strlen(path) > 0;
}

void split_default(int in_dsc, char** outs, int num)
{
  pthread_t *threads;
  thread_arg_t *args;
  pthread_mutex_t mutex;
  int i;
  threads = malloc(sizeof(pthread_t)*num);
  args = malloc(sizeof(thread_arg_t)*num);
  pthread_mutex_init(&mutex, NULL);
  for (i =0; i < num; ++i) {
    if (outs[i] != NULL) {
      args[i].in_dsc = in_dsc;
      args[i].out_dsc = open(outs[i], O_WRONLY);
      if (args[i].out_dsc >= 0) {
        args[i].mutex = &mutex;
        pthread_create(threads + i, NULL, &pomp, (void*)(args + i));
      }
    }
  }

  for (i =0; i < num; ++i) {
    if (outs[i] != NULL && args[i].out_dsc >= 0) {
      pthread_join(threads[i], NULL);
      close(args[i].out_dsc);
    }
  }

  free(args);
  free(threads);
}

void *pomp(void *arg)
{
  thread_arg_t *arg_ = (thread_arg_t*) arg;
  int buff_size = DEFAULT_BUFF_SIZE;
  char *buffer = malloc(sizeof(char)*buff_size);
  char c;
  do {
    int n = 0;
    pthread_mutex_lock(arg_->mutex);
    do {
      if (read(arg_->in_dsc, &c, 1) != 1) {
        c = EOF;
        break;
      }

      buffer[n] = c;
      ++n;
      if (n >= buff_size) {
        char *tmp = malloc(sizeof(char)*buff_size*2);
        memcpy(tmp, buffer, sizeof(char)*buff_size);
        free(buffer);
        buffer = tmp;
        buff_size *= 2;
      }
    } while (c != '\n' && c != '\0' && c != EOF);
    pthread_mutex_unlock(arg_->mutex);

    if (write(arg_->out_dsc, buffer, n) == -1) {
      break;
    }
  } while (c != '\0' && c != EOF);

  free(buffer);
}

void split_sequential(int in_dsc, char** outs, int num)
{
  int i;
  char c;
  int *out_dscs = malloc(sizeof(int)*num);
  for (i = 0; i < num; ++i) {
      out_dscs[i] = open(outs[i], O_WRONLY);
  }

  i = 0;
  do {
    do {
      if (out_dscs[i] >= 0) {
        if (read(in_dsc, &c, 1) == 1) {
          if (write(out_dscs[i], &c, 1) == -1) {
            break;
          }
        } else {
          c = EOF;
        }
      }
    } while (c != '\n' && c != EOF);
    ++i;
    i %= num;
  } while (c != EOF);

  for (i = 0; i < num; ++i) {
    if (out_dscs[i] >= 0) {
      close(out_dscs[i]);
    }
  }

  free(out_dscs);
}

void exec_splitter(char *cmd, int in_dsc, char** outs, int num)
{
  char *outs_str;
  char *cmd_;
  int len = 0;
  int i;
  for (i = 0; i < num; ++i) {
    len += strlen(outs[i]) + 1;
  }

  outs_str = malloc(sizeof(char)*len + 1);
  cmd_ = malloc(sizeof(char)*(strlen(cmd) + len));
  strjoin(outs_str, outs, num, " ");
  strrep(cmd_, cmd, "{}", outs_str);
  free(outs_str);
  if (strlen(cmd_) == 0) {
    return;
  }

  dup2(in_dsc, 0);
  execlp("sh", "sh", "-c", cmd_, NULL);
}

void exec_mapper(char *cmd, int in_dsc, int out_dsc)
{
  dup2(in_dsc, 0);
  dup2(out_dsc, 1);
  execlp("sh", "sh", "-c", cmd, NULL);
}

void exec_combiner(char *cmd, char **ins, int out_dsc, int num)
{
  char *ins_str;
  char *cmd_;
  int len = 0;
  int i;
  for (i = 0; i < num; ++i) {
    len += strlen(ins[i]) + 1;
  }

  ins_str = malloc(sizeof(char)*len + 1);
  cmd_ = malloc(sizeof(char)*(strlen(cmd) + len));
  strjoin(ins_str, ins, num, " ");
  strrep(cmd_, cmd, "{}", ins_str);
  free(ins_str);
  if (strlen(cmd_) == 0) {
    return;
  }

  dup2(out_dsc, 0);
  execlp("sh", "sh", "-c", cmd_, NULL);
}

void combine_default(char** ins, int out_dsc, int num)
{
  pthread_t *threads;
  thread_arg_t *args;
  int i;
  pthread_mutex_t mutex;
  threads = malloc(sizeof(pthread_t)*num);
  args = malloc(sizeof(thread_arg_t)*num);
  pthread_mutex_init(&mutex, NULL);
  for (i =0; i < num; ++i) {
    if (ins[i] != NULL) {
      args[i].in_dsc = open(ins[i], O_RDONLY);
      args[i].out_dsc = out_dsc;
      if (args[i].in_dsc >= 0) {
        args[i].mutex = &mutex;
        pthread_create(threads + i, NULL, &pomp, (void*)(args + i));
      }
    }
  }

  for (i =0; i < num; ++i) {
    if (ins[i] != NULL && args[i].in_dsc >= 0) {
      pthread_join(threads[i], NULL);
      close(args[i].in_dsc);
    }
  }

  free(args);
  free(threads);
}


void combine_sequential(char** ins, int out_dsc, int num)
{
  int i;
  char c;
  int *in_dscs = malloc(sizeof(int)*num);
  for (i = 0; i < num; ++i) {
      in_dscs[i] = open(ins[i], O_RDONLY);
  }

  i = 0;
  do {
    do {
      if (in_dscs[i] >= 0) {
        if (read(in_dscs[i], &c, 1) == 1) {
          if (write(out_dsc, &c, 1) == -1) {
            break;
          }
        } else {
          c = EOF;
        }
      }
    } while (c != '\n' && c != EOF);
    ++i;
    i %= num;
  } while (c != EOF);

  for (i = 0; i < num; ++i) {
    if (in_dscs[i] >= 0) {
      close(in_dscs[i]);
    }
  }

  free(in_dscs);
}

int strrep(char *dest, char *src, char *before, char *after)
{
  char *p;
  int pos;
  int src_len;
  int before_len;
  int after_len;
  if (strlen(before) == 0) {
    strcpy(dest, src);
    return FALSE;
  }

  p = strstr(src, before);
  if (p == NULL) {
    strcpy(dest, src);
    return FALSE;
  }

  pos = p - src;
  src_len = strlen(src);
  before_len = strlen(before);
  after_len = strlen(after);
  memcpy(dest, src, pos);
  dest += p - src;
  memcpy(dest, after, after_len);
  dest += after_len;
  memcpy(dest, p + before_len, src_len - before_len - pos + 1);
  return TRUE;
}

int strjoin(char *dest, char **src, int num, char *delim)
{
  int delim_len = strlen(delim);
  int i;
  for (i = 0; i < num;) {
    strcpy(dest, src[i]);
    dest += strlen(src[i]);
    ++i;
    if (i < num) {
      strcpy(dest, delim);
      dest += delim_len;
    }
  }
}

void wait_all()
{
  int status;
  while (TRUE) {
    if (wait(&status) == -1
        && errno == ECHILD) {
      break;
    }
  }
}

void cleanup_tmpdir()
{
  if (is_valid_dir(tmpdir_name)) {
    int i;
    wait_all();
    for (i = 0; in_fifos[i] != '\0'; ++i) {
      unlink(in_fifos[i]);
      unlink(out_fifos[i]);
    }
    rmdir(tmpdir_name);
    tmpdir_name = NULL;
  }
}

void trap_int()
{
  if (parent_proc) {
    cleanup_tmpdir();
  }
  exit(-1);
}
