import 'dart:async';

/// Cancellation is scoped to one HTTP reader, not the whole audio/video track.
class StreamJob {
  final _cancelled = Completer<void>();
  final Set<void Function()> _listeners = {};
  bool get cancelled => _cancelled.isCompleted;
  Future<void> get whenCancelled => _cancelled.future;

  void check() {
    if (cancelled) throw const StreamCancelled();
  }

  void listen(void Function() listener) {
    if (cancelled) {
      listener();
    } else {
      _listeners.add(listener);
    }
  }

  void unlisten(void Function() listener) => _listeners.remove(listener);

  void cancel() {
    if (cancelled) return;
    _cancelled.complete();
    for (final listener in List.of(_listeners)) {
      listener();
    }
    _listeners.clear();
  }
}

class StreamCancelled implements Exception {
  const StreamCancelled();
}

class _Ticket {
  _Ticket(this.job, this.priority);
  final StreamJob job;
  final int priority;
  final ready = Completer<void>();
}

/// Shared by all proxies. A permit lasts until the upstream body is consumed.
class StreamScheduler {
  StreamScheduler(this.limit, {this.reserveUrgent = false});
  final bool reserveUrgent;
  int limit;
  int active = 0;
  int _normal = 0;
  int peak = 0;
  final List<_Ticket> _queue = [];

  Future<void Function()> acquire(StreamJob job, {int priority = 0}) async {
    job.check();
    final ticket = _Ticket(job, priority);
    final index = _queue.indexWhere((entry) => entry.priority < priority);
    if (index < 0) {
      _queue.add(ticket);
    } else {
      _queue.insert(index, ticket);
    }
    void cancel() {
      if (_queue.remove(ticket)) {
        ticket.ready.completeError(const StreamCancelled());
      }
    }

    job.listen(cancel);
    _drain();
    try {
      await ticket.ready.future;
    } finally {
      job.unlisten(cancel);
    }
    var released = false;
    void release() {
      if (released) return;
      released = true;
      active--;
      if (ticket.priority == 0) _normal--;
      _drain();
    }

    if (job.cancelled) {
      release();
      throw const StreamCancelled();
    }
    return release;
  }

  void _drain() {
    while (active < limit && _queue.isNotEmpty) {
      final index = _queue.indexWhere(
        (entry) => !reserveUrgent || entry.priority > 0 || _normal < limit - 1,
      );
      if (index < 0) return;
      final ticket = _queue.removeAt(index);
      if (ticket.job.cancelled) {
        ticket.ready.completeError(const StreamCancelled());
      } else {
        active++;
        if (ticket.priority == 0) _normal++;
        if (active > peak) peak = active;
        ticket.ready.complete();
      }
    }
  }
}
