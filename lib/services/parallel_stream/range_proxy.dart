// Keep this transport independently runnable without Flutter/pub dependencies.
// ignore_for_file: always_use_package_imports
import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'cdn_resolver.dart';
import 'scheduler.dart';

enum ParallelFailure { unavailable, invalidResponse, timeout, rateLimited }

class ParallelStreamException implements Exception {
  const ParallelStreamException(this.kind);
  final ParallelFailure kind;
  @override
  String toString() => 'Parallel stream: ${kind.name}';
}

class ParallelStats {
  int networkBytes = 0;
  int deliveredBytes = 0;
  int retries = 0;
  int active = 0;
  int peak = 0;
  int bufferedBytes = 0;
  int peakBufferedBytes = 0;
  Map<String, int> toJson() => {
    'networkBytes': networkBytes,
    'deliveredBytes': deliveredBytes,
    'retries': retries,
    'active': active,
    'peak': peak,
    'bufferedBytes': bufferedBytes,
    'peakBufferedBytes': peakBufferedBytes,
  };
}

class _Track {
  _Track(this.candidates, this.audio, this.identity);
  final List<Uri> candidates;
  final bool audio;
  final String identity;
  Uri? pinned;
  int? total;
  String? strongEtag;
  Future<void>? probing;
}

class _Piece {
  _Piece(this.bytes, this.total, this.etag);
  final Uint8List bytes;
  final int total;
  final String? etag;
}

/// A bounded, seekable byte source for mpv. It never changes codecs or timing.
class ParallelRangeProxy {
  ParallelRangeProxy({
    this.concurrency = 8,
    required this.userAgent,
    this.referer = 'https://www.bilibili.com/',
    this.onFailure,
    this.attemptTimeout = const Duration(seconds: 10),
    this.stallTimeout = const Duration(seconds: 4),
    this.allowLoopbackForTesting = false,
  }) {
    if (![4, 8, 16].contains(concurrency)) {
      throw ArgumentError.value(concurrency, 'concurrency');
    }
  }

  final int concurrency;
  final String userAgent;
  final String referer;
  final void Function(ParallelFailure)? onFailure;
  final Duration attemptTimeout;
  final Duration stallTimeout;
  // Explicit, off by default; production never accepts HTTP/private targets.
  final bool allowLoopbackForTesting;
  final stats = ParallelStats();
  static final _requests = StreamScheduler(8, reserveUrgent: true);
  // Eight 2 MiB windows shared across sessions: at most 16 MiB of media blocks.
  static final _windows = StreamScheduler(8);
  final _client = HttpClient()..autoUncompress = false;
  final Map<String, _Track> _tracks = {};
  final Set<StreamJob> _jobs = {};
  final Set<HttpResponse> _responses = {};
  final _lifetime = StreamJob();
  HttpServer? _server;
  bool _closed = false;
  bool _failed = false;
  bool _paused = false;
  Completer<void>? _resume;
  final _token = List.generate(
    24,
    (_) => Random.secure().nextInt(256),
  ).map((byte) => byte.toRadixString(16).padLeft(2, '0')).join();

  Future<void> start() async {
    _lifetime.check();
    _requests.limit = concurrency;
    _client.connectionTimeout = attemptTimeout;
    _client.idleTimeout = const Duration(seconds: 15);
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    if (_closed) {
      await server.close(force: true);
      throw const StreamCancelled();
    }
    _server = server;
    server.listen((request) {
      // A peer can disconnect even while a 404/405 is being written.
      unawaited(_serve(request).catchError((Object _) {}));
    });
  }

  bool _allowed(Uri uri) =>
      isParallelMediaUri(uri) ||
      (allowLoopbackForTesting &&
          uri.scheme == 'http' &&
          uri.host == '127.0.0.1' &&
          uri.userInfo.isEmpty);

  String register(
    List<Uri> candidates, {
    required String identity,
    bool audio = false,
  }) {
    _lifetime.check();
    final allowed = candidates.where(_allowed).toSet().toList();
    if (allowed.isEmpty || _server == null) {
      throw const ParallelStreamException(ParallelFailure.unavailable);
    }
    final path = '/$_token/${_tracks.length}';
    _tracks[path] = _Track(allowed, audio, identity);
    return 'http://127.0.0.1:${_server!.port}$path';
  }

  Future<void> prepare() async {
    for (final track in _tracks.values) {
      await _ensureProbe(track);
    }
  }

  void setPaused(bool paused) {
    _paused = paused;
    if (!paused) {
      _resume?.complete();
      _resume = null;
    }
  }

  Future<void> _waitPlaying(StreamJob job) async {
    while (_paused) {
      _resume ??= Completer<void>();
      await Future.any([_resume!.future, job.whenCancelled]);
      job.check();
    }
  }

  /// Cancel old readers on seek while preserving the pinned track identity.
  void cancelReaders() {
    for (final job in List.of(_jobs)) {
      job.cancel();
    }
    for (final response in List.of(_responses)) {
      unawaited(_breakResponse(response));
    }
  }

  void close() {
    if (_closed) return;
    _closed = true;
    _lifetime.cancel();
    setPaused(false);
    cancelReaders();
    _client.close(force: true);
    final server = _server;
    _server = null;
    if (server != null) unawaited(server.close(force: true));
    _tracks.clear();
  }

  Future<void> _ensureProbe(_Track track) => track.probing ??= _probe(track);

  Future<void> _probe(_Track track) async {
    // Probe a bounded number before player.open. Each is also cancellable by
    // the session deadline in the caller. Once pinned, never silently mix CDNs.
    for (final uri in track.candidates) {
      _lifetime.check();
      try {
        final piece = await _fetch(uri, 0, 0, _lifetime, priority: 2);
        track
          ..pinned = uri
          ..total = piece.total
          ..strongEtag = piece.etag;
        return;
      } on StreamCancelled {
        rethrow;
      } on ParallelStreamException catch (error) {
        if (error.kind == ParallelFailure.rateLimited) rethrow;
      } catch (_) {
        /* Try another candidate before any media is exposed. */
      }
    }
    throw const ParallelStreamException(ParallelFailure.unavailable);
  }

  Future<_Piece> _fetch(
    Uri uri,
    int start,
    int end,
    StreamJob job, {
    required int priority,
  }) async {
    final release = await _requests.acquire(job, priority: priority);
    HttpClientRequest? request;
    var finished = false;
    var expired = false;
    void abort() => request?.abort(const StreamCancelled());
    job.listen(abort);
    final deadline = Timer(attemptTimeout, () {
      expired = true;
      request?.abort(const ParallelStreamException(ParallelFailure.timeout));
    });
    stats.active++;
    stats.peak = max(stats.peak, stats.active);
    try {
      request = await _client
          .getUrl(uri)
          .then((value) {
            if (expired || job.cancelled) value.abort(const StreamCancelled());
            return value;
          })
          .timeout(attemptTimeout);
      job.check();
      if (expired) throw const ParallelStreamException(ParallelFailure.timeout);
      request.followRedirects = false;
      request.headers
        ..set(HttpHeaders.rangeHeader, 'bytes=$start-$end')
        ..set(HttpHeaders.acceptEncodingHeader, 'identity')
        ..set(HttpHeaders.userAgentHeader, userAgent)
        ..set(HttpHeaders.refererHeader, referer);
      final response = await request.close().timeout(attemptTimeout);
      if (response.statusCode == 429) {
        // Stop this session rather than bypass a rate limit via another CDN.
        throw const ParallelStreamException(ParallelFailure.rateLimited);
      }
      final match = RegExp(r'^bytes (\d+)-(\d+)/(\d+)$').firstMatch(
        response.headers.value(HttpHeaders.contentRangeHeader) ?? '',
      );
      final encoding = response.headers.value(
        HttpHeaders.contentEncodingHeader,
      );
      if (response.statusCode != 206 ||
          match == null ||
          int.tryParse(match[1]!) != start ||
          int.tryParse(match[2]!) != end ||
          (encoding != null && encoding != 'identity')) {
        throw const ParallelStreamException(ParallelFailure.invalidResponse);
      }
      final total = int.tryParse(match[3]!);
      if (total == null || total <= end || total > 9007199254740991) {
        throw const ParallelStreamException(ParallelFailure.invalidResponse);
      }
      final length = end - start + 1;
      if (response.contentLength >= 0 && response.contentLength != length) {
        throw const ParallelStreamException(ParallelFailure.invalidResponse);
      }
      final bytes = Uint8List(length);
      var offset = 0;
      await for (final chunk in response.timeout(stallTimeout)) {
        job.check();
        stats.networkBytes += chunk.length;
        if (offset + chunk.length > length) {
          throw const ParallelStreamException(ParallelFailure.invalidResponse);
        }
        bytes.setRange(offset, offset + chunk.length, chunk);
        offset += chunk.length;
      }
      if (offset != length) {
        throw const ParallelStreamException(ParallelFailure.invalidResponse);
      }
      finished = true;
      final etag = response.headers.value(HttpHeaders.etagHeader);
      return _Piece(
        bytes,
        total,
        etag != null && !etag.startsWith('W/') ? etag : null,
      );
    } finally {
      deadline.cancel();
      job.unlisten(abort);
      if (!finished) request?.abort(const StreamCancelled());
      stats.active--;
      release();
    }
  }

  Future<_Piece> _download(
    _Track track,
    int start,
    int end,
    StreamJob job,
  ) async {
    final clock = Stopwatch()..start();
    for (var attempt = 0; attempt < 3; attempt++) {
      job.check();
      try {
        final piece = await _fetch(
          track.pinned!,
          start,
          end,
          job,
          priority: track.audio ? 2 : 0,
        );
        if (piece.total != track.total ||
            (track.strongEtag != null && piece.etag != track.strongEtag)) {
          throw const ParallelStreamException(ParallelFailure.invalidResponse);
        }
        return piece;
      } on StreamCancelled {
        rethrow;
      } catch (error) {
        job.check();
        if (error is ParallelStreamException &&
            error.kind != ParallelFailure.timeout) {
          rethrow;
        }
        if (attempt == 2 || clock.elapsedMilliseconds >= 18000) {
          throw const ParallelStreamException(ParallelFailure.timeout);
        }
        stats.retries++;
        await Future.any([
          Future<void>.delayed(Duration(milliseconds: 250 * (attempt + 1))),
          job.whenCancelled,
        ]);
      }
    }
    throw const ParallelStreamException(ParallelFailure.unavailable);
  }

  static (int, int) parseRange(String? range, int total) {
    if (total <= 0) throw const FormatException('Invalid length');
    if (range == null || range.contains(',')) return (0, total - 1);
    final match = RegExp(r'^bytes=(\d*)-(\d*)$').firstMatch(range);
    if (match == null || (match[1]!.isEmpty && match[2]!.isEmpty)) {
      throw const FormatException('Invalid range');
    }
    if (match[1]!.isEmpty) {
      final suffix = int.tryParse(match[2]!);
      if (suffix == null || suffix <= 0) {
        throw const FormatException('Invalid suffix');
      }
      return (max(0, total - suffix), total - 1);
    }
    final start = int.tryParse(match[1]!);
    final last = match[2]!.isEmpty ? total - 1 : int.tryParse(match[2]!);
    if (start == null || last == null || start >= total || last < start) {
      throw const FormatException('Unsatisfiable range');
    }
    return (start, min(last, total - 1));
  }

  Future<void> _serve(HttpRequest request) async {
    final response = request.response;
    final track = _tracks[request.uri.path];
    if (_closed ||
        track == null ||
        request.uri.hasQuery ||
        request.headers.value(HttpHeaders.hostHeader) !=
            '127.0.0.1:${_server?.port}') {
      response.statusCode = 404;
      await response.close();
      return;
    }
    if (request.method != 'GET' && request.method != 'HEAD') {
      response.statusCode = 405;
      response.headers.set(HttpHeaders.allowHeader, 'GET, HEAD');
      await response.close();
      return;
    }
    final job = StreamJob();
    _jobs.add(job);
    _responses.add(response);
    unawaited(
      response.done.then((_) => job.cancel(), onError: (_) => job.cancel()),
    );
    var sent = false;
    try {
      await _ensureProbe(track);
      job.check();
      final range = request.method == 'HEAD'
          ? null
          : request.headers.value(HttpHeaders.rangeHeader);
      final (start, end) = parseRange(range, track.total!);
      final partial = range != null && !range.contains(',');
      response.statusCode = partial ? 206 : 200;
      response.headers
        ..set(HttpHeaders.acceptRangesHeader, 'bytes')
        ..set(HttpHeaders.cacheControlHeader, 'no-store')
        ..set(HttpHeaders.contentTypeHeader, 'application/octet-stream');
      if (partial) {
        response.headers.set(
          HttpHeaders.contentRangeHeader,
          'bytes $start-$end/${track.total}',
        );
      }
      response.contentLength = end - start + 1;
      if (request.method == 'GET') {
        var cursor = start;
        var first = true;
        while (cursor <= end) {
          job.check();
          await _waitPlaying(job);
          final releaseWindow = await _windows.acquire(
            job,
            priority: track.audio ? 1 : 0,
          );
          final futures = <Future<(_Piece?, Object?)>>[];
          var heldBytes = 0;
          try {
            final windowEnd = min(
              end,
              cursor + (first ? 64 * 1024 : 2 * 1024 * 1024) - 1,
            );
            final pieceSize = first
                ? 64 * 1024
                : track.audio || concurrency == 16
                ? 128 * 1024
                : 256 * 1024;
            while (cursor <= windowEnd) {
              final pieceEnd = min(windowEnd, cursor + pieceSize - 1);
              heldBytes += pieceEnd - cursor + 1;
              futures.add(
                _download(
                  track,
                  cursor,
                  pieceEnd,
                  job,
                ).then<(_Piece?, Object?)>(
                  (piece) => (piece, null),
                  onError: (Object error) => (null, error),
                ),
              );
              cursor = pieceEnd + 1;
            }
            stats.bufferedBytes += heldBytes;
            stats.peakBufferedBytes = max(
              stats.peakBufferedBytes,
              stats.bufferedBytes,
            );
            for (final future in futures) {
              final (piece, error) = await future;
              job.check();
              if (error != null) throw error;
              response.add(piece!.bytes);
              sent = true;
              try {
                await response.flush();
              } catch (_) {
                throw const StreamCancelled();
              }
              stats.deliveredBytes += piece.bytes.length;
            }
          } catch (_) {
            job.cancel();
            rethrow;
          } finally {
            // Do not release the memory reservation while children still run.
            await Future.wait(futures);
            stats.bufferedBytes -= heldBytes;
            releaseWindow();
          }
          first = false;
        }
      }
      await response.close();
    } on FormatException {
      if (!sent) {
        response.statusCode = 416;
        response.headers.set(
          HttpHeaders.contentRangeHeader,
          'bytes */${track.total}',
        );
        response.contentLength = 0;
        await response.close();
      }
    } catch (error) {
      // Client disconnect/seek is not a network failure and must not trigger fallback.
      if (!_closed && error is! StreamCancelled && !_failed) {
        _failed = true;
        onFailure?.call(
          error is ParallelStreamException
              ? error.kind
              : ParallelFailure.unavailable,
        );
      }
      if (error is StreamCancelled) {
        await _breakResponse(response);
      } else if (!sent) {
        try {
          response
            ..statusCode = 502
            ..contentLength = 0;
          await response.close();
        } catch (_) {
          /* Peer already gone. */
        }
      } else {
        await _breakResponse(response);
      }
    } finally {
      job.cancel();
      _jobs.remove(job);
      _responses.remove(response);
    }
  }

  static Future<void> _breakResponse(HttpResponse response) async {
    try {
      // detachSocket is illegal after headers have been written. A deadline
      // destroys the connection even after a partial response was flushed.
      response.deadline = Duration.zero;
      await response.done;
    } catch (_) {
      /* Already detached or closed by mpv. */
    }
  }
}
