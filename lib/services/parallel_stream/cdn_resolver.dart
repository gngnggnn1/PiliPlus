/// Candidate selection only. A track is pinned once its first valid byte arrives.
/// This avoids concatenating unproven copies of a resource from different CDNs.
enum ParallelCdnMode { existing, mainland, overseas }

const _mainland = [
  'upos-sz-mirrorali.bilivideo.com',
  'upos-sz-mirrorhw.bilivideo.com',
  'upos-sz-mirrorcos.bilivideo.com',
];
const _overseas = [
  'upos-sz-mirroraliov.bilivideo.com',
  'upos-sz-mirrorcosov.bilivideo.com',
  'upos-sz-mirrorhwov.bilivideo.com',
];

bool isParallelMediaUri(Uri uri) =>
    uri.scheme == 'https' &&
    uri.userInfo.isEmpty &&
    !uri.hasFragment &&
    (uri.host == 'bilivideo.com' ||
        uri.host.endsWith('.bilivideo.com') ||
        uri.host.endsWith('.bilivideo.cn') ||
        uri.host.endsWith('.bilivideo.net') ||
        uri.host == 'upos-hz-mirrorakam.akamaized.net') &&
    RegExp(r'\.(m4s|mp4)$', caseSensitive: false).hasMatch(uri.path);

List<Uri> parallelCandidates({
  required String direct,
  required Iterable<String> originals,
  required ParallelCdnMode mode,
}) {
  final api = originals
      .map(Uri.tryParse)
      .whereType<Uri>()
      .where(isParallelMediaUri)
      .toList();
  final selected = Uri.tryParse(direct);
  final result = <Uri>[];
  if (mode != ParallelCdnMode.existing && api.isNotEmpty) {
    // Only rewrite an ordinary Bilibili resource; never assume an Akamai
    // signature authorizes another host. Rewritten URLs must pass a real probe.
    final donors = api.where((uri) => uri.host.endsWith('.bilivideo.com'));
    if (donors.isNotEmpty) {
      for (final host
          in mode == ParallelCdnMode.mainland ? _mainland : _overseas) {
        result.add(donors.first.replace(host: host, port: 443));
      }
    }
  }
  if (selected != null && isParallelMediaUri(selected)) result.add(selected);
  result.addAll(api);
  return result.toSet().take(8).toList(growable: false);
}
