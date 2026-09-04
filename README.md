# web-proxy

EdgeOne / 反代前置的容器镜像与 GitHub 加速器。单二进制 axum + reqwest + rustls 实现。

## 路由

| 路径 | 用途 |
|---|---|
| `GET /healthz` | 健康检查，需 `X-Origin-Secret` 头 |
| `GET /v2/` | Docker Registry v2 ping；无凭据回 401 + Bearer challenge，`Authorization: Bearer edge-registry-probe` 直返 200 |
| `GET /token` | 重写 `docker.io/` / `ghcr.io/` scope 前缀后转发到 `auth.docker.io` / `ghcr.io`，**不**转发客户端 Authorization |
| `GET /v2/{docker.io\|ghcr.io}/<path>` | 流式转发到 `registry-1.docker.io` / `ghcr.io`；401 时回写带 scope 的 challenge |
| `GET\|POST /<github-host>/<path>` | 流式转发到 9 个白名单 GitHub 域名 |

白名单（`GITHUB_HOSTS`）：`github.com`, `api.github.com`, `raw.githubusercontent.com`, `codeload.github.com`, `gist.github.com`, `gist.githubusercontent.com`, `objects.githubusercontent.com`, `release-assets.githubusercontent.com`, `pkg-containers.githubusercontent.com`。

## 配置（环境变量）

| 变量 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `LISTEN_ADDR` | 否 | `0.0.0.0:20516` | 监听地址（`host:port`） |
| `PUBLIC_BASE_URL` | 是 | — | 对外暴露的 HTTPS origin，无路径/查询/凭据；用于构造 token challenge |
| `ORIGIN_SECRET` | 是 | — | ≥32 字节；客户端用 `X-Origin-Secret` 头传入 |
| `MAX_CONCURRENT_REQUESTS` | 否 | `128` | 全局并发上限 |
| `MAX_REDIRECTS` | 否 | `8` | 上游重定向跟随上限 |
| `UPSTREAM_CONNECT_TIMEOUT_SECS` | 否 | `10` | reqwest 连接超时 |
| `SHUTDOWN_DRAIN_TIMEOUT_SECS` | 否 | `30` | 收到停止信号后等待在途请求排空的上限，超时强制断开 |
| `RUST_LOG` | 否 | `info` | tracing 日志级别 |

## 部署

```bash
cp .env.example .env
# 编辑 .env：填 PUBLIC_BASE_URL 和 ORIGIN_SECRET（openssl rand -base64 48）
docker compose up -d
```

`compose.yaml` 已加固：`read_only: true` + `cap_drop: [ALL]` + `pids_limit: 512` + `cpus: 2.0` + `mem_limit: 512m` + `no-new-privileges`。

## 安全模型

- **SSRF 防护**：拒绝 IP 字面量、非 443 端口、userinfo、路径穿越；上游白名单精确匹配
- **重定向**：每跳重新校验主机白名单；跨主机自动剥离 `Authorization`
- **POST 降级**：301/302/303 的 POST 改 GET 且丢弃 body；307/308 保留方法与 body
- **凭据转发**：
  - `/token` 路由**不**转发客户端 Authorization（公网模式）
  - `/v2/<registry>/<path>` **会**转发客户端 Authorization 给上游 registry，便于拉私有镜像
  - 若部署为纯公网加速，建议上游使用只读 token 或匿名
- **体积/超时**：请求体上限 64 MB；token 响应缓冲上限 1 MB；其余流式不落盘；上游读超时 1800s

## 客户端配置示例

Docker daemon（`daemon.json`）：
```json
{
  "registry-mirrors": ["https://accel.example.com"]
}
```

`/etc/containers/registries.conf`（podman / buildah）：
```toml
[[registry]]
location = "accel.example.com"
```

Git clone：
```bash
git clone https://accel.example.com/github.com/owner/repo.git
```

## 开发

```bash
cargo test          # 7 个单元测试覆盖路由、scope 重写、SSRF 白名单、定时比较
cargo fmt --all -- --check
```

Git Bash 下 `link.exe` 与 MSVC linker 冲突，无法本地构建；推送后由 CI 验证。

## 已知限制

- `rewrite_scope` 仅接受 `pull` 单动作；不支持 `push`
- streaming 响应无总体积上限（单 blob 受 EdgeOne 边缘层限制）
- graceful shutdown 排空上限默认 30s（`SHUTDOWN_DRAIN_TIMEOUT_SECS`），超时后剩余连接被强制断开

## LICENSE

未声明。公仓使用前请补 LICENSE 文件（推荐 MIT 或 Apache-2.0）。
