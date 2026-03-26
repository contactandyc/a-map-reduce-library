// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "amr_cli.h"
#include "amr_internal.h"
#include "a-memory-library/aml_pool.h"
#include "the-io-library/io.h"

#include <stdio.h>
#include <string.h>

void schedule_usage(amr_t *h) {
  if (h->custom_usage) {
    h->custom_usage();
    printf("\n----------------------------------------------------------\n\n");
  }
  printf("The scheduler is meant to aid in running tasks in parallel.\n");
  printf("At the moment, it operates on a single host - but I'm planning\n");
  printf("on improving it to support multiple computers.\n");
  printf("\n\n");

  printf("--run <N> specifies the iterative run namespace (defaults to 0).\n");
  printf("   Must be enabled via amr_use_runs() in the application.\n\n");

  printf("--keep-files prevents the framework from automatically deleting\n");
  printf("   intermediate outputs after they are consumed.\n\n");

  printf("--debug <task:partition> <output path> - run a single task in\n");
  printf("   isolated environment\n\n");
  printf("-f|--force rerun selected tasks even if they don't need run\n\n");
  printf("-t <task[:partitions]>[<task[:partitions]], select tasks and\n");
  printf("   optionally partitions.  tasks are separated by vertical bars\n");
  printf(
      "   (|) and partitions are sub-selected by placing a colon and then\n");
  printf("   the partitions.  The partitions can be a single partition\n");
  printf("   number, arange separated by a - (1-3), or a list of single\n");
  printf(
      "   partitions or ranges separated by commas.  To select partitions\n");
  printf("   1, 3, 4, and 5 of task named first_task\n");
  printf("   -t first_task:1,3-5\n\n");
  printf("-o  Normally, all tasks that are needed to run to complete\n");
  printf("    selected task will run.  This will override that behavior\n");
  printf("    and only run selected tasks\n\n");
  printf(
      "-d|--dump <filename1,[filename2],...> dump the contents of files\n\n");
  printf(
      "-p|--prefix <filename1,[filename2],...> dump the contents of files\n");
  printf("    and prefix each line with the line number\n\n");
  printf("--sample <records>:<partitions> efficiently peeks into task outputs,\n");
  printf("    reports data skew (empty partitions), and dumps N records from\n");
  printf("    M randomly selected active partitions.\n\n");

  printf("--match <string> lazily filters output of --sample and --dump to only\n");
  printf("    include records containing the target string.\n\n");

  printf("-l|--list list details of execution (the plan)\n\n");
  printf("-s|--show-files similar to list, except input and output files\n");
  printf("     are also displayed.\n\n");
  printf("-c|--cpus <num_cpus> overrides default number of cpus\n\n");
  printf("-r|--ram <ram MB> overrides default ram usage\n\n");
  printf("-h|--help show this help message\n\n");
}

void parse_args(amr_t *h) {
  parsed_args_t at;
  memset(&at, 0, sizeof(at));

  /* Preserve the run_number from the early pre-scan in amr_use_runs() */
  at.run_number = h->parsed_args.run_number;
  at.keep_files = h->parsed_args.keep_files;

  char *filenames = NULL;
  char *tasks = NULL;

  char *debug_task = NULL;

  char **p = h->args;
  char **ep = p + h->argc;
  bool should_read_args = true;
  char **custom_args =
      (char **)aml_pool_zalloc(h->pool, sizeof(char *) * (h->argc + 1));
  size_t num_custom_args = 0;

  while (p < ep) {
    if (!strcmp(*p, "-f") || !(strcmp(*p, "--force"))) {
      at.force = true;
      p++;
    } else if (!strcmp(*p, "--run")) {
      if (h->use_runs) {
        p += 2; /* Safely skip the flag and its value */
      } else {
        printf("[ERROR] --run used but amr_use_runs() was not called by the application\n\n");
        at.help = true;
        p = ep;
      }
    } else if (!strcmp(*p, "--keep-files")) {
      at.keep_files = true;
      p++;
    } else if (!strcmp(*p, "--debug")) {
      p++;
      if (p + 2 <= ep) {
        debug_task = *p;
        p++;
        at.debug_path = *p;
        p++;
        if (p + 1 <= ep) {
          if (!strcmp(*p, "dump_input")) {
            at.debug_dump_input = true;
            p++;
          }
        }
      } else {
        at.help = true;
        p = ep;
      }
    } else if (!strcmp(*p, "-o")) {
      at.only_run_selected = true;
      p++;
    } else if (!strcmp(*p, "--new-args")) {
      should_read_args = false;
      p++;
    } else if (!strcmp(*p, "-l") || !(strcmp(*p, "--list"))) {
      at.list = true;
      at.dump = false;
      at.prefix = false;
      p++;
    } else if (!strcmp(*p, "-s") || !(strcmp(*p, "--show-files"))) {
      at.list = true;
      at.show_files = true;
      at.dump = false;
      at.prefix = false;
      p++;
    } else if (!strcmp(*p, "--sample")) {
      at.sample = true;
      at.dump = true;
      p++;
      if (p == ep || sscanf(*p, "%zu:%zu", &at.sample_records, &at.sample_partitions) != 2) {
        printf("[ERROR] --sample requires <records>:<partitions> (e.g., --sample 3:2)\n\n");
        at.help = true;
      } else {
        p++;
      }
    } else if (!strcmp(*p, "--match")) {
      p++;
      if (p == ep || (*p)[0] == '-') {
        printf("[ERROR] --match requires a string to be listed after parameter\n\n");
        at.help = true;
      } else {
        at.sample_match = *p;
        p++;
      }
    } else if (!strcmp(*p, "-h") || !(strcmp(*p, "--help"))) {
      at.help = true;
      p++;
    } else if (!strcmp(*p, "-d") || !(strcmp(*p, "--dump")) ||
               !strcmp(*p, "-p") || !(strcmp(*p, "--prefix"))) {
      at.list = false;
      at.show_files = false;
      at.dump = true;
      char *ptr = *p;
      if (ptr[1] == 'p' || (ptr[1] == '-' && ptr[2] == 'p'))
        at.prefix = true;
      p++;
      if (p == ep || (*p)[0] == '-') {
        printf("[ERROR] --dump and --prefix requires one or more files to be "
               "listed after parameter\n\n");
        at.help = true;
      } else {
        while (p < ep && (*p)[0] != '-') {
          filenames =
              aml_pool_strdupf(h->pool, "%s%s%s", filenames ? filenames : "",
                               filenames ? "," : "", *p);
          p++;
        }
      }
    } else if (!strcmp(*p, "-t") || !(strcmp(*p, "--task"))) {
      p++;
      if (p == ep || (*p)[0] == '-') {
        printf("[ERROR] --task requires one or more tasks to be listed after "
               "parameter\n\n");
        at.help = true;
      } else {
        while (p < ep && (*p)[0] != '-') {
          tasks = aml_pool_strdupf(h->pool, "%s%s%s", tasks ? tasks : "",
                                   tasks ? "|" : "", *p);
          p++;
        }
      }
    } else if (!strcmp(*p, "-r") || !strcmp(*p, "--ram")) {
      p++;
      if (p == ep) {
        printf("[ERROR] --ram requires MB to be listed after parameter\n\n");
        at.help = true;
      } else {
        if (sscanf(*p, "%zu", &at.ram) != 1)
          at.help = true;
        p++;
      }
    } else if (!strcmp(*p, "-c") || !strcmp(*p, "--cpus")) {
      p++;
      if (p == ep) {
        printf("[ERROR] --cpus requires number of cpus to be listed after "
               "parameter\n\n");
        at.help = true;
      } else {
        if (sscanf(*p, "%zu", &at.cpus) != 1)
          at.help = true;
        p++;
      }
    } else {
      if (h->parse_args) {
        int n = h->parse_args(ep - p, p, h->parse_args_arg);
        if (n <= 0) {
          printf("[ERROR] Invalid custom argument %s\n\n", *p);
          at.help = true;
          p = ep;
        } else {
          while (n) {
            custom_args[num_custom_args] = *p;
            p++;
            num_custom_args++;
            n--;
          }
        }
      } else {
        printf("[ERROR] Invalid argument %s\n\n", *p);
        at.help = true;
        p = ep;
      }
    }
  }
  if (h->parse_args) {
    char *custom_args_file =
        aml_pool_strdupf(h->pool, "%s/custom_args", h->task_dir);
    char **old_args = NULL;
    size_t num_old_args = 0;
    if (should_read_args) {
      size_t len;
      char *buf = io_read_file(&len, custom_args_file);
      if (buf) {
        old_args = aml_pool_split(h->pool, &num_old_args, '\n', buf);
        aml_free(buf);
      }
    }

    /* compare old args with new args */
    if (!num_old_args) {
      // go ahead and write new args
      if (num_custom_args) {
        io_make_directory(h->task_dir); /* Ensure directory exists before writing args! */
        FILE *out = fopen(custom_args_file, "wb");
        for (size_t i = 0; i < num_custom_args; i++)
          fprintf(out, "%s\n", custom_args[i]);
        fclose(out);
      }
    } else if (!num_custom_args) {
      custom_args = old_args;
      num_custom_args = num_old_args;
      p = custom_args;
      ep = p + num_custom_args;
      while (p < ep) {
        int n = h->parse_args(ep - p, p, h->parse_args_arg);
        if (n <= 0) {
          printf("[ERROR] Invalid custom argument %s\n\n", *p);
          at.help = true;
          p = ep;
        } else
          p += n;
      }
    } else {
      p = old_args;
      ep = p + num_old_args;
      char **p2 = custom_args;
      char **ep2 = p2 + num_custom_args;
      while (p < ep && p2 < ep2) {
        if (strcmp(*p, *p2)) {
          printf("[ERROR] Command line arguments don\'t match (%s != %s) - "
                 "(use --new-args?)\n\n",
                 *p, *p2);
          at.help = true;
          break;
        }
        p++;
        p2++;
      }
      if (!at.help) {
        if (p < ep) {
          printf("[ERROR] Command line arguments don\'t match (more old args "
                 "%s(%zu) "
                 "than new (%zu)) - (use --new-args?)\n\n",
                 *p, num_old_args, num_custom_args);
          at.help = true;
        } else if (p2 < ep2) {
          printf("[ERROR] Command line arguments don\'t match (more new args "
                 "%s(%zu) "
                 "than old (%zu)) - (use --new-args?)\n\n",
                 *p2, num_old_args, num_custom_args);
          at.help = true;
        }
      }
    }
  }

  at.num_selected = 0;
  at.select_all = true;
  at.files = aml_pool_split(h->pool, NULL, ',', filenames);
  if (!at.help && tasks) {
    at.select_all = false;
    size_t num_t = 0;
    char **t = aml_pool_split(h->pool, &num_t, '|', tasks);
    if (num_t) {
      selected_task_t *selected = (selected_task_t *)aml_pool_zalloc(
          h->pool, sizeof(selected_task_t) * num_t);
      at.tasks = selected;
      at.num_tasks = num_t;
      for (size_t i = 0; i < num_t; i++) {
        char *p = strchr(t[i], ':');
        if (p) {
          *p++ = 0;
        }
        selected[i].task = amr_find_task(h, t[i]); // <--- Changed to public API
        if (!selected[i].task) {
          at.help = true;
          break;
        }
        size_t num_partitions = selected[i].task->num_partitions;
        selected[i].partitions =
            (bool *)aml_pool_zalloc(h->pool, sizeof(bool) * num_partitions);
        if (p) {
          char **parts = &p;
          char **eparts = parts + 1;

          if (strchr(p, ',')) {
            size_t num_parts = 0;
            parts = aml_pool_split(h->pool, &num_parts, ',', p);
            eparts = parts + num_parts;
          }
          while (!at.help && parts < eparts) {
            p = *parts;
            parts++;
            if (strchr(p, '-')) {
              size_t p1, p2;
              if (sscanf(p, "%zu-%zu", &p1, &p2) != 2 || p1 >= num_partitions ||
                  p2 >= num_partitions || p2 < p1) {
                at.help = true;
                break;
              } else {
                while (p1 <= p2) {
                  if (!selected[i].partitions[p1]) {
                    selected[i].partitions[p1] = true;
                    at.num_selected++;
                  }
                  p1++;
                }
              }
            } else {
              size_t p1;
              if (sscanf(p, "%zu", &p1) != 1 || p1 >= num_partitions) {
                at.help = true;
                break;
              } else {
                if (!selected[i].partitions[p1]) {
                  selected[i].partitions[p1] = true;
                  at.num_selected++;
                }
                p1++;
              }
            }
          }
        } else {
          for (size_t p1 = 0; p1 < num_partitions; p1++) {
            if (!selected[i].partitions[p1]) {
              selected[i].partitions[p1] = true;
              at.num_selected++;
            }
          }
        }
      }
    }
  }
  if (!at.help && at.tasks) {
    for (size_t i = 0; i < at.num_tasks; i++)
      at.tasks[i].task->selected = at.tasks[i].partitions;
  }
  if (!at.help && debug_task) {
    debug_task = aml_pool_strdup(h->pool, debug_task);
    char *p = strchr(debug_task, ':');
    if (!p) {
      printf("[ERROR]: debug task partition not specified %s\n\n", debug_task);
      at.help = true;
    } else {
      *p++ = 0;
      at.debug_task = amr_find_task(h, debug_task); // <--- Changed to public API
      if (!at.debug_task) {
        at.help = true;
        printf("[ERROR]: debug task not found %s\n\n", debug_task);
      } else {
        if (sscanf(p, "%zu", &at.debug_partition) != 1 ||
            at.debug_partition >= at.debug_task->num_partitions) {
          printf("[ERROR]: debug task partition not valid %s\n\n", p);
          at.help = true;
        } else
          io_make_directory(at.debug_path);
      }
      if (!at.help) {
        h->parsed_args.cpus = 1;
      }
    }
  }

  if (at.dump || at.list || at.sample || at.help) {
    at.keep_files = true;
  }

  h->parsed_args = at;
}
