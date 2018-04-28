Author: Nipun Sehrawat (nipun.sehrawat.ns@gmail.com, nipun@thoughtspot.com)

# Threadstacks
ThreadStacks can be used to programatically inspect stacktraces of all threads of a live process that links against the ThreadStacks library. Roughly speaking, ThreadStacks provides the equivalent of Golang's `runtime.Stack()` for C/C++ programs. Besides programatic access to stacktraces, ThreadStacks also provides `jmap` style utility, where `kill -35` can be used to have a live process write stacktraces of all its threads to stderr.

ThreadStacks has been used by ThoughtSpot's production services since early 2015 and has become a staple debugging tool for both test and production environments. It has helped us debug a variety of issues in some of the most critical services in our stack, including our in-memory database and cluster manager. Some of the interesting issues include stuck database queries because of lock contention, dysfunctional cluster manager scheduler due to stuck HDFS reads, and deadlocked processes due to buggy recursive locking of `boost::shared_mutex`.

## Goal
The main goal of ThreadStacks is to have the ability to inspect stacktraces of a live process, without pausing, stopping, or affecting its execution in any non-trivial way. ThreadStacks is used in critical backend services at ThoughtSpot, so having a bug-free implementation which guarantees safe, crash/corruption free invocation is paramount.

## Design
ThreadStacks collects stacktraces of threads in a live process by using POSIX realtime signals. Realtime signals have two advantages over vanilla POSIX signals - they are queued, and they can carry a payload. Both of these features are crucial to ThreadStacks’s implementation, as described below. Writing correct signal handlers is tricky because of the async-signal safety requirements, but this restriction is what makes writing signal handlers fun - one has to come up with innovative solutions and workarounds for the restrictive environment. For example, it is unsafe to allocate memory from a signal handler, as most malloc() implementations are non-reentrant and thus are not async-signal-safe. Infact, POSIX enumerates only a handful of system calls that are guaranteed to be async-signal-safe.

![Thread Stacktrace Collection Algorithm](https://github.com/thoughtspot/threadstacks/blob/master/resources/ThreadStacks.jpg)

The following steps are executed to collect stacktraces of threads:
1. Find the list of threads running in the process (T1, T2, T3). This is done by getting children of ‘/proc/self/task’ directory.
2. Allocate a memory slot for each thread to write its stacktrace (M1, M2, M3). Note that allocating memory from signal handlers is not async-signal-safe, hence memory is allocated beforehand.
3. Send a realtime signal to each discovered thread and wait for their acks. The corresponding memory slot and an ack file descriptor is part of the payload of the realtime signal.
4. Threads’ signal handler uses libunwind to compute their stacktraces and write raw instruction pointer based stacktrace in their respective memory slots.
5. After writing its stack trace in the designated memory slot, each thread acks back to collector thread over a pipe.
6. On receiving all the acks (same as the number of threads detected in step #1), collector thread uniquifies and symbolizes the stacktraces.

## Usage
ThreadStacks library can be used to inspect stacktraces of threads of a live process. A process can link against ThreadStacks library and install two of the signal handlers defined in 'StackTraceSignal' class to have the ability to live inspect its stacktraces:

```
thoughtspot::StackTraceSignal::InstallInternalHandler()
thoughtspot::StackTraceSignal::InstallExternalHandler()
```

After the above two signal handlers have been installed, the 'StackTraceCollector' class can be used to collect stacktraces, e.g. from a REST handler.

Installation of 'external' signal handler also ensures that the process dumps stacktraces of all threads to stderr on receiving signal 35, e.g. from a `kill -35` command.

## Building
ThreadStacks uses bazel as its build system and depends on 'glog', 'gflags', and 'googletests' projects, as remote bazel projects.

The code in ThreadStacks repository can be built by running:
```
bazel build //threadstacks/...
```
and tested by running:
```
bazel test //...
```
