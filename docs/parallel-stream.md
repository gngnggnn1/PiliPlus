# PiliPlus Parallel（Android 实验版）

基于 [PiliPlus](https://github.com/bggRGjQaUbCoE/PiliPlus) 的个人 fork。
受到 [Bilibili-thread-ripper](https://github.com/MrTangLuyao/Bilibili-thread-ripper) 并发分段加载思路启发；保留上游许可和署名。

## 使用

在「设置 → 音视频设置」中开启「并发加载（实验性）」，重新打开视频。默认关闭、仅 Wi-Fi、最多 8 个连接，连接数可选择 4 / 8 / 16。先使用 8，不稳定时尝试 4。关闭开关会让当前视频回到普通播放；连接数和 CDN 模式在下次打开视频或切换画质时生效。

可选择跟随原有 CDN 设置、优先大陆节点或优先海外节点。节点可用性受地区、网络和签名影响，没有普遍最快的选项。既有「音频不跟随 CDN 设置」启用时，音频保留直连。

仅处理 Android 普通 UGC DASH 视频。直播、离线文件、非 DASH、番剧/课程及已标记 DRM 的视频沿用上游播放方式。并发加载不改变账号权限或可选画质，也不保证能改善每一种网络的速度。蜂窝网络默认不参与；关闭 Wi-Fi 限制可能增加流量和耗电。Wi-Fi 检测失败时保守停用；系统 VPN 等复杂路由仍需实机验证。

应用名称为 **PiliPlus Parallel**，包名为 `io.github.gngnggnn1.piliplus.parallel`，数据与官方版分开。Android 的检查更新按钮打开本 fork 的 Releases，避免下载官方安装包覆盖此版本。

## 实现

播放器仍使用现有 media_kit/mpv。页面提供当前 CID、视频画质/编码、音轨和该 representation 的原始候选 URL；Dart 在 `127.0.0.1` 的随机端口提供带随机令牌的 HTTP 地址，mpv 读取这个地址。

在开始播放前对候选发起 `bytes=0-0` 探测，检查 206 和 Content-Range。启动探测最多等待 6 秒，失败使用原地址。确认一个节点后整条音轨/视频轨固定在该节点；不会将不同 CDN 上未经证明一致的字节拼在一起。429 直接结束本次加速，不切节点规避限流。

首块为 64 KiB，其后使用 2 MiB 滑动窗口，分成 128/256 KiB 小块并行读取、按字节顺序交付。所有代理共享连接调度器；预留一个连接给音频/探测，因此单视频通常最多使用配置数减一。并发 16 时视频使用 128 KiB 块。重试同样占用连接配额。八个窗口合计最多保留 16 MiB 媒体块，此数不包含 mpv、HTTP socket、Dart VM 自身缓存。

每个分段校验起止位置、总长度、响应编码、实际长度，以及探测时存在的强 ETag。断网/超时重试最多两次；单次总时限 10 秒、无数据时限 4 秒。播放中失败会撤销代理并最多回退一次，重新打开原始直连资源，恢复进度、暂停状态、倍速、音量和字幕轨。seek 取消旧读者；暂停后不再申请新窗口，已在传输的窗口可以完成。更换视频、画质、网络策略和退出时关闭旧会话。

地址只接受预设 Bilibili 媒体域名，代理不接受客户端提供的目标 URL，也不跟随上游重定向。统计仅保存字节数、连接数、重试次数及缓冲量，不记录 Cookie、签名 URL 或视频内容。

## 本地验证与构建

Flutter 版本按 `pubspec.yaml`（当前 3.47.4）；Android 使用 JDK 17 或兼容版本、API 37.0、对应 Build Tools 和 Flutter 所需 NDK。Windows 的 pub cache 建议使用短路径，Git 开启当前进程的 `core.longpaths`。

```powershell
$env:PUB_CACHE = 'C:\dev\pili-pub'
$env:GIT_CONFIG_COUNT = '1'
$env:GIT_CONFIG_KEY_0 = 'core.longpaths'
$env:GIT_CONFIG_VALUE_0 = 'true'
flutter pub get
./tool/patch_android.ps1 -FlutterRoot C:\dev\flutter -PubCache $env:PUB_CACHE
dart tool/test_parallel_stream.dart
flutter analyze --no-pub
flutter build apk --release --target-platform android-arm64 --no-pub
```

`patch_android.ps1` 使用上游 Android 补丁，但不修改全局 Git 身份、不重置 Flutter SDK，也不删除 pub 缓存。请使用独立的 Flutter SDK 和 pub cache。重复执行时检测已应用的补丁；冲突会停止。

发布用签名通过上游 `android/key.properties` 配置，密钥和密码不能提交。未配置时沿用上游的 debug 签名回退；这种 APK 仅用于测试，不同机器生成的测试签名不能互相覆盖安装。

协议测试不需要 Flutter 引擎、B 站账号或真实视频，覆盖字节完整性、Range/HEAD/416、域名策略、队列取消、音视频共用连接预算、200/403/429/错误范围/短响应、总长/ETag 变化、暂停、seek 和 URL 撤销。CI 对此分支执行同一套协议测试。

## 尚需 Android 实机验证

本地 HTTP 测试不能证明真实海外网络速度，也不能替代 mpv/系统后台行为测试。发布稳定版前至少检查：

- 同一视频、同一画质/编码对比开关关闭与开启：首帧时间、拖动恢复时间、卡顿次数、流量、温度。
- 快速连续拖动、切换画质/音轨、切换视频，确认不会播放旧资源或重复回退。
- 暂停后恢复、锁屏后台音频、画中画、蓝牙与耳机控制，确认音画同步和后台生命周期正常。
- 播放中关闭开关、Wi-Fi 切蜂窝、断网再联网，确认能回退且进度/暂停状态保留。
- 字幕、倍速、杜比/HDR、音频直连选项；直播、离线、番剧不受影响。

发现问题时先关闭实验开关。提交复现步骤时提供设备、系统、网络、连接数和 CDN 模式；不要贴 Cookie 或带签名参数的播放 URL。
