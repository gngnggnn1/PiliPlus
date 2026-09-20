// Dependency-free protocol regression suite: dart tool/test_parallel_stream.dart
// ignore_for_file: avoid_relative_lib_imports, cascade_invocations
import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import '../lib/services/parallel_stream/cdn_resolver.dart';
import '../lib/services/parallel_stream/range_proxy.dart';
import '../lib/services/parallel_stream/scheduler.dart';

void check(bool value, String reason) {
  if (!value) throw StateError(reason);
}

Future<void> rejects(Future<Object?> future, String reason) async {
  var threw = false;
  try {
    await future;
  } catch (_) {
    threw = true;
  }
  check(threw, reason);
}

class Fixture {
  final bytes = Uint8List.fromList(
    List.generate(3 * 1024 * 1024 + 19, (i) => i % 251),
  );
  late HttpServer server;
  int active = 0;
  int peak = 0;
  int calls = 0;
  final List<int> starts = [];
  String fault = '';
  Duration delay = const Duration(milliseconds: 5);
  Uri get uri => Uri.parse('http://127.0.0.1:${server.port}/video.m4s');

  Future<void> start() async {
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    server.listen((request) async {
      active++;
      calls++;
      if (active > peak) peak = active;
      try {
        final match = RegExp(r'bytes=(\d+)-(\d+)')
            .firstMatch(request.headers.value('range')!);
        final first = int.parse(match![1]!);
        final last = int.parse(match[2]!);
        starts.add(first);
        await Future<void>.delayed(delay);
        final response = request.response;
        if (fault == '200' && last > 0) {
          response.statusCode = 200;
          response.contentLength = 3;
          response.add([1, 2, 3]);
        } else if (fault == '403' || fault == '429') {
          response.statusCode = int.parse(fault);
          response.contentLength = 0;
        } else {
          response.statusCode = 206;
          var total = bytes.length;
          if (fault == 'total' && first > 0) total++;
          final returnedFirst = fault == 'range' && last > 0
              ? first + 1
              : first;
          response.headers.set(
            'content-range',
            'bytes $returnedFirst-$last/$total',
          );
          response.headers.set(
            'etag',
            fault == 'etag' && first > 0 ? '"b"' : '"a"',
          );
          final content = bytes.sublist(
            first,
            last + (fault == 'short' && last > 0 ? 0 : 1),
          );
          response.contentLength = content.length;
          response.add(content);
        }
        await response.close();
      } catch (_) {
        /* The proxy deliberately aborts rejected/obsolete requests. */
      } finally {
        active--;
      }
    });
  }

  Future<void> close() => server.close(force: true);
}

class Harness {
  final fixture = Fixture();
  final failures = <ParallelFailure>[];
  final client = HttpClient();
  late ParallelRangeProxy proxy;
  late Uri video;
  Future<void> start({int concurrency = 8}) async {
    await fixture.start();
    proxy = ParallelRangeProxy(
      concurrency: concurrency,
      userAgent: 'test',
      allowLoopbackForTesting: true,
      onFailure: failures.add,
      attemptTimeout: const Duration(milliseconds: 800),
      stallTimeout: const Duration(milliseconds: 400),
    );
    await proxy.start();
    video = Uri.parse(proxy.register([fixture.uri], identity: 'video:1'));
    await proxy.prepare();
  }

  Future<(int, List<int>, String?)> read({
    String? range,
    Uri? uri,
    String method = 'GET',
  }) async {
    final request = await client.openUrl(method, uri ?? video);
    if (range != null) request.headers.set('range', range);
    final response = await request.close();
    final bytes = await response.fold<List<int>>(
      [],
      (all, chunk) => all..addAll(chunk),
    );
    return (
      response.statusCode,
      bytes,
      response.headers.value('content-range'),
    );
  }

  Future<void> close() async {
    client.close(force: true);
    proxy.close();
    await fixture.close();
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
}

Future<void> main() async {
  var passed = 0;
  Future<void> test(String name, Future<void> Function() body) async {
    await body().timeout(const Duration(seconds: 15));
    stdout.writeln('PASS $name');
    passed++;
  }

  Future<void> withHarness(
    Future<void> Function(Harness h) body, {
    int concurrency = 8,
  }) async {
    final h = Harness();
    await h.start(concurrency: concurrency);
    try {
      await body(h);
    } finally {
      await h.close();
    }
  }

  await test('range parser: bounded, open, suffix and invalid', () async {
    check(
      ParallelRangeProxy.parseRange('bytes=4-99', 10) == (4, 9),
      'clamp EOF',
    );
    check(ParallelRangeProxy.parseRange('bytes=4-', 10) == (4, 9), 'open');
    check(ParallelRangeProxy.parseRange('bytes=-3', 10) == (7, 9), 'suffix');
    check(
      ParallelRangeProxy.parseRange('bytes=0-1,4-5', 10) == (0, 9),
      'multi-range ignored',
    );
    for (final value in [
      'bytes=10-',
      'bytes=-0',
      'bytes=5-4',
      'bytes=-',
      'garbage',
    ]) {
      var invalid = false;
      try {
        ParallelRangeProxy.parseRange(value, 10);
      } on FormatException {
        invalid = true;
      }
      check(invalid, 'must reject $value');
    }
  });
  await test(
    'candidate policy rejects arbitrary hosts and credentials',
    () async {
      check(
        !isParallelMediaUri(Uri.parse('https://evil.akamaized.net/a.m4s')),
        'shared CDN',
      );
      check(
        !isParallelMediaUri(Uri.parse('https://u:p@a.bilivideo.com/a.m4s')),
        'credentials',
      );
      final candidates = parallelCandidates(
        direct: 'https://a.bilivideo.com/a.m4s?token=x',
        originals: ['https://a.bilivideo.com/a.m4s?token=x'],
        mode: ParallelCdnMode.mainland,
      );
      check(
        candidates.length == 4 && candidates.every((u) => u.query == 'token=x'),
        'signed path retained',
      );
    },
  );
  await test('scheduler cancellation and urgent reservation', () async {
    final scheduler = StreamScheduler(4, reserveUrgent: true);
    final job = StreamJob();
    final releases = <void Function()>[];
    for (var i = 0; i < 3; i++) {
      releases.add(await scheduler.acquire(job));
    }
    final waiting = StreamJob();
    final future = scheduler.acquire(waiting);
    final urgent = await scheduler.acquire(job, priority: 2);
    check(scheduler.active == 4, 'urgent permit');
    final result = rejects(future, 'cancelled queued task must finish');
    waiting.cancel();
    await result;
    urgent();
    for (final release in releases) {
      release();
    }
    check(scheduler.active == 0, 'all permits returned');
  });
  await test(
    'parallel full byte stream is identical and bounded',
    () => withHarness((h) async {
      final (status, bytes, _) = await h.read();
      check(
        status == 200 && bytes.length == h.fixture.bytes.length,
        'full response',
      );
      for (var i = 0; i < bytes.length; i++) {
        check(bytes[i] == i % 251, 'wrong byte $i');
      }
      check(
        h.proxy.stats.peak > 1 && h.proxy.stats.peak <= 8,
        'real bounded concurrency',
      );
      check(
        h.proxy.stats.peakBufferedBytes <= 2 * 1024 * 1024,
        'window memory',
      );
      check(h.failures.isEmpty, 'no fallback');
    }),
  );
  await test(
    'suffix, open-ended, HEAD, 416, unknown token and methods',
    () => withHarness((h) async {
      final (suffixStatus, tail, contentRange) = await h.read(
        range: 'bytes=-19',
      );
      check(
        suffixStatus == 206 && tail.length == 19 && contentRange != null,
        'tail',
      );
      final (_, open, _) = await h.read(
        range: 'bytes=${h.fixture.bytes.length - 9}-',
      );
      check(open.length == 9, 'open ended');
      final (headStatus, head, _) = await h.read(method: 'HEAD');
      check(headStatus == 200 && head.isEmpty, 'HEAD');
      final (invalid, _, invalidRange) = await h.read(
        range: 'bytes=${h.fixture.bytes.length}-',
      );
      check(
        invalid == 416 && invalidRange == 'bytes */${h.fixture.bytes.length}',
        '416',
      );
      check(
        (await h.read(uri: h.video.replace(path: '/unknown'))).$1 == 404,
        'token',
      );
      check((await h.read(method: 'POST')).$1 == 405, 'method');
    }),
  );
  for (final fault in ['200', 'range', 'short', '403', '429']) {
    await test(
      'reject $fault before first media byte, emit one failure',
      () => withHarness((h) async {
        h.fixture.fault = fault;
        final (status, bytes, _) = await h.read(range: 'bytes=0-100');
        check(status == 502 && bytes.isEmpty, 'no invalid bytes');
        check(h.failures.length == 1, 'explicit fallback notification');
      }),
    );
  }
  for (final fault in ['total', 'etag']) {
    await test(
      'reject changed $fault after headers without successful truncation',
      () => withHarness((h) async {
        h.fixture.fault = fault;
        await rejects(h.read(), 'partial stream must fail');
        check(h.failures.length == 1, 'fallback after partial response');
      }),
    );
  }
  await test(
    'audio/video/readers share the total request limit',
    () => withHarness((h) async {
      final audio = Uri.parse(
        h.proxy.register([h.fixture.uri], identity: 'audio:1', audio: true),
      );
      final results = await Future.wait([
        h.read(range: 'bytes=0-2097151'),
        h.read(range: 'bytes=60000-1700000'),
        h.read(uri: audio, range: 'bytes=0-400000'),
      ]);
      check(results.every((r) => r.$1 == 206), 'all readers succeed');
      check(h.proxy.stats.peak <= 8 && h.fixture.peak <= 8, 'shared budget');
      check(h.proxy.stats.bufferedBytes == 0, 'memory returned');
    }),
  );
  await test(
    'seek cancellation releases tasks without fallback',
    () => withHarness((h) async {
      h.fixture.delay = const Duration(milliseconds: 300);
      final result = rejects(h.read(), 'cancelled reader should stop');
      await Future<void>.delayed(const Duration(milliseconds: 30));
      h.proxy.cancelReaders();
      await result;
      await Future<void>.delayed(const Duration(milliseconds: 30));
      check(
        h.proxy.stats.active == 0 && h.failures.isEmpty,
        'cancel is not failure',
      );
      h.fixture.delay = Duration.zero;
      check(
        (await h.read(range: 'bytes=1000-2000')).$2.length == 1001,
        'new seek succeeds',
      );
    }),
  );
  await test(
    'pause stops new downloads and resume progresses',
    () => withHarness((h) async {
      h.proxy.setPaused(true);
      final previous = h.fixture.calls;
      final future = h.read(range: 'bytes=0-100');
      await Future<void>.delayed(const Duration(milliseconds: 50));
      check(h.fixture.calls == previous, 'paused means no new upstream work');
      h.proxy.setPaused(false);
      check((await future).$2.length == 101, 'resumed');
    }),
  );
  await test(
    'a pinned track does not switch to another unverified CDN',
    () => withHarness((h) async {
      final secondary = Fixture();
      await secondary.start();
      try {
        final source = Uri.parse(
          h.proxy.register([h.fixture.uri, secondary.uri], identity: 'pinned'),
        );
        await h.proxy.prepare();
        h.fixture.fault = '200';
        check((await h.read(uri: source)).$1 == 502, 'fail pinned source');
        check(secondary.calls == 0, 'do not splice other source');
      } finally {
        await secondary.close();
      }
    }),
  );
  await test(
    'closed session URL is revoked',
    () => withHarness((h) async {
      h.proxy.close();
      await rejects(h.read(), 'old URL must stop working');
    }),
  );
  for (final concurrency in [4, 16]) {
    await test(
      'connection budget $concurrency is enforced and usable',
      () => withHarness((h) async {
        final (_, bytes, _) = await h.read();
        check(bytes.length == h.fixture.bytes.length, 'all bytes delivered');
        check(h.proxy.stats.peak <= concurrency, 'connection budget exceeded');
        if (concurrency == 16)
          check(
            h.proxy.stats.peak > 8,
            '16 mode must use additional connections',
          );
      }, concurrency: concurrency),
    );
  }
  await test(
    'temporary timeout retries and recovers',
    () => withHarness((h) async {
      h.fixture.delay = const Duration(milliseconds: 900);
      final timer = Timer(
        const Duration(milliseconds: 100),
        () => h.fixture.delay = Duration.zero,
      );
      try {
        final (status, bytes, _) = await h.read(range: 'bytes=0-100');
        check(status == 206 && bytes.length == 101, 'retry must recover');
        check(
          h.proxy.stats.retries == 1 && h.failures.isEmpty,
          'one retry, no fallback',
        );
      } finally {
        timer.cancel();
      }
    }),
  );
  await test(
    'persistent timeout terminates with one failure',
    () => withHarness((h) async {
      h.fixture.delay = const Duration(milliseconds: 900);
      final (status, _, _) = await h.read(range: 'bytes=0-100');
      check(status == 502 && h.failures.length == 1, 'finite failure');
      check(
        h.proxy.stats.retries == 2 && h.proxy.stats.active == 0,
        'bounded retries and permits released',
      );
    }),
  );
  await test(
    'rate limited probe does not try the next CDN',
    () => withHarness((h) async {
      final secondary = Fixture();
      await secondary.start();
      try {
        h.fixture.fault = '429';
        h.proxy.register([
          h.fixture.uri,
          secondary.uri,
        ], identity: 'rate-limited');
        await rejects(h.proxy.prepare(), 'probe must fail on rate limit');
        check(secondary.calls == 0, 'no bypass via another CDN');
      } finally {
        await secondary.close();
      }
    }),
  );
  stdout.writeln('$passed protocol tests passed.');
}
