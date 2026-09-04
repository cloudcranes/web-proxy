# web-proxy

局域网自托管的容器镜像拉取网关（类 KSpeeder 的 M1 阶段实现）：内容寻址磁盘缓存 + Docker Registry v2 代理 + GitHub 加速。单二进制 axum + reqwest + rustls。

## 功能（M1）

- **Docker Hub 镜像模式**：daemon `registry-mirrors` 指向本网关，自动完成 token 挑战/换取闭环
- **blob 磁盘缓存**：`sha256:<digest>` 即缓存键（OCI blob 不可变），流式边下边存，sha256 校验通过才原子落盘；同 digest 并发请求单飞（single-flight），客户端断连也会继续拉完种子缓存
- **manifest 内存缓存**：60s TTL，按 Accept 协商头分键
- **LRU 容量淘汰**：`CACHE_MAX_GB` 超限后按 mtime 淘汰最旧 blob（90% 水位）
- **GHCR 代理**：`/v2/ghcr.io/...` 路径前缀模式（镜像 tag 改写为 `gateway:20516/ghcr.io/owner/img`）
- **GitHub 加速**：9 个白名单域名透传（release/raw/codeload/api，git clone 支持）
- **`/stats`**：缓存命中率、上下行字节、磁盘占用
- **安全**：出站仅限白名单域名（上游 + 重定向 CDN），拒绝 IP 字面量/非 443/路径穿越；`registry-mirrors` 场景下 blob/manifest 不落公网

## 配置

| 变量 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `LISTEN_ADDR` | 否 | `0.0.0.0:20516` | 监听地址 |
| `CACHE_DIR` | 否 | `/data` | blob 缓存目录（compose 已挂数据卷） |
| `CACHE_MAX_GB` | 否 | `10` | blob 缓存容量上限 |
| `MANIFEST_TTL_SECS` | 否 | `60` | manifest 内存缓存 TTL |
| `MANIFEST_CACHE_ENTRIES` | 否 | `2048` | manifest 缓存条目上限 |
| `MAX_CONCURRENT_REQUESTS` | 否 | `128` | 全局并发上限 |
| `MAX_REDIRECTS` | 否 | `8` | 上游重定向跟随上限 |
| `UPSTREAM_CONNECT_TIMEOUT_SECS` | 否 | `10` | 上游连接超时 |
| `SHUTDOWN_DRAIN_TIMEOUT_SECS` | 否 | `30` | 停止后排空在途请求的上限 |
| `PUBLIC_ORIGIN` | 否 | 从请求 Host 推导 | token challenge 的 realm 前缀 |
| `DOCKERHUB_REGISTRY_URL` | 否 | `https://registry-1.docker.io` | Docker Hub 上游 |
| `DOCKERHUB_TOKEN_URL` | 否 | `https://auth.docker.io/token` | Docker Hub token 端点 |
| `DOCKERHUB_TOKEN_SERVICE` | 否 | `registry.docker.io` | token service 参数 |
| `GHCR_REGISTRY_URL` / `GHCR_TOKEN_URL` / `GHCR_TOKEN_SERVICE` | 否 | ghcr.io 官方 | GHCR 上游 |
| `REDIRECT_HOSTS` | 否 | 空 | 允许 blob 重定向落到的额外 CDN 域名（逗号分隔） |
| `RUST_LOG` | 否 | `info` | 日志级别 |

上游直连受限的网络（如国内），把 `DOCKERHUB_*` 指向可达的镜像源即可（见 `.env.example` 注释）。

## 部署

```bash
cp .env.example .env   # 按需修改上游
docker compose up -d
curl http://<host>:20516/healthz   # → ok
```

compose 已加固：`read_only` + `cap_drop: [ALL]` + `pids_limit: 512` + `cpus/mem_limit` + `no-new-privileges`，缓存落在 `blob-cache` 数据卷。

## 客户端接入

Docker daemon（`daemon.json`，HTTP 模式需加 insecure-registries）：

```json
{
  "registry-mirrors": ["http://192.168.1.107:20516"],
  "insecure-registries": ["192.168.1.107:20516"]
}
```

GHCR / GitHub 走路径前缀：

```bash
docker pull 192.168.1.107:20516/ghcr.io/owner/image:tag   # 需同样加入 insecure-registries
git clone http://192.168.1.107:20516/github.com/owner/repo.git
```

## 开发

```bash
cargo fmt --all -- --check
cargo test
```

本机无 MSVC 工具链时由 CI 验证（fmt + check + test + 多架构 buildx）。

## 已知限制（M1）

- 仅 HTTP 监听；TLS mirror 端口在后续阶段
- 不支持 push（scope 只收 `pull`）
- 带 `Range` 头的 blob 请求直通不缓存
- 直连 Docker Hub 不可达时需配置可达上游（官方/镜像源均可）

## LICENSE

[MIT](LICENSE)
