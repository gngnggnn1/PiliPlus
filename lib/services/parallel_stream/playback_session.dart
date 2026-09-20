import 'dart:async';
import 'dart:io';

import 'package:PiliPlus/http/browser_ua.dart';
import 'package:PiliPlus/plugin/pl_player/models/data_source.dart';
import 'package:PiliPlus/utils/storage.dart';
import 'package:PiliPlus/utils/storage_key.dart';
import 'package:PiliPlus/utils/storage_pref.dart';
import 'package:connectivity_plus/connectivity_plus.dart';

import 'package:PiliPlus/services/parallel_stream/cdn_resolver.dart';
import 'package:PiliPlus/services/parallel_stream/range_proxy.dart';
import 'package:PiliPlus/services/parallel_stream/scheduler.dart';

/// Owns one representation's proxy and network policy subscriptions.
class ParallelPlaybackSession {
  ParallelPlaybackSession(this.onFallback);
  final void Function() onFallback;
  ParallelRangeProxy? proxy;
  final List<StreamSubscription<dynamic>> _subscriptions = [];
  bool _closed = false;

  static bool get enabled => Platform.isAndroid && Pref.parallelStreamEnabled;

  static bool _wifi(List<ConnectivityResult> result) =>
      result.contains(ConnectivityResult.wifi) &&
      !result.contains(ConnectivityResult.mobile);

  Future<bool> _networkAllowed() async {
    if (!Pref.parallelStreamWifiOnly) return true;
    try {
      return _wifi(
        await Connectivity().checkConnectivity().timeout(
          const Duration(seconds: 1),
        ),
      );
    } catch (_) {
      return false;
    }
  }

  void _fallback() {
    if (_closed) return;
    // Stop traffic immediately, even while player.open is still pending.
    close();
    onFallback();
  }

  Future<(String, String?)?> prepare(
    NetworkSource source, {
    required bool audioOnly,
  }) async {
    if (!enabled ||
        source.parallelIdentity == null ||
        !await _networkAllowed() ||
        _closed) {
      return null;
    }
    final mode = ParallelCdnMode.values[Pref.parallelStreamCdnMode];
    // Preserve custom/unsupported sources rather than replacing them with a
    // different URL from the API metadata.
    if (!isParallelMediaUri(Uri.tryParse(source.videoSource) ?? Uri()))
      return null;
    if (audioOnly &&
        (Pref.disableAudioCDN || source.audioSource?.isNotEmpty != true)) {
      return null;
    }
    final videoCandidates = parallelCandidates(
      direct: source.videoSource,
      originals: source.videoCandidates,
      mode: mode,
    );
    if (!audioOnly && videoCandidates.isEmpty) return null;
    final worker = proxy = ParallelRangeProxy(
      concurrency: Pref.parallelStreamConnections,
      userAgent: BrowserUa.pc,
      onFailure: (_) => _fallback(),
    );
    try {
      await worker.start();
      if (_closed) throw const StreamCancelled();
      var video = source.videoSource;
      var audio = source.audioSource;
      if (!audioOnly) {
        video = worker.register(
          videoCandidates,
          identity: '${source.parallelIdentity}:video',
        );
      }
      if (audio != null && audio.isNotEmpty && !Pref.disableAudioCDN) {
        final candidates = parallelCandidates(
          direct: audio,
          originals: source.audioCandidates,
          mode: mode,
        );
        if (candidates.isNotEmpty) {
          audio = worker.register(
            candidates,
            identity: '${source.parallelIdentity}:audio',
            audio: true,
          );
        } else if (audioOnly) {
          close();
          return null;
        }
      }
      // A bounded startup cost; a slow/unsupported CDN keeps the original path.
      await worker.prepare().timeout(const Duration(seconds: 6));
      if (_closed || !enabled || !await _networkAllowed()) {
        throw const StreamCancelled();
      }
      _subscriptions.add(
        Connectivity().onConnectivityChanged.listen(
          (result) {
            if (Pref.parallelStreamWifiOnly && !_wifi(result)) _fallback();
          },
          onError: (Object _) {
            if (Pref.parallelStreamWifiOnly) _fallback();
          },
        ),
      );
      _subscriptions.add(
        GStorage.setting.watch().listen((event) async {
          if (event.key == SettingBoxKey.parallelStreamEnabled && !enabled) {
            _fallback();
          } else if (event.key == SettingBoxKey.parallelStreamWifiOnly &&
              !await _networkAllowed()) {
            _fallback();
          }
        }),
      );
      if (_closed || !enabled) {
        for (final subscription in _subscriptions) {
          unawaited(subscription.cancel());
        }
        _subscriptions.clear();
        return null;
      }
      return (video, audio);
    } catch (_) {
      close();
      return null;
    }
  }

  void close() {
    if (_closed) return;
    _closed = true;
    for (final subscription in _subscriptions) {
      unawaited(subscription.cancel());
    }
    _subscriptions.clear();
    proxy?.close();
  }
}
