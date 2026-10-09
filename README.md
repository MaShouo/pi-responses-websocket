# pi-responses-websocket

通用 **OpenAI Responses WebSocket** provider，使用 API Key，不需要在 Pi 登录 OAuth。

- 自动识别 provider ID 以 `-ws` 或 `_ws` 结尾的配置，不区分大小写，例如 `OpenAI-ws`、`OpenAI-WS`、`GPT-WS`、`OpenAI_ws`、`GPT_WS`；兼容旧名称 `responses-websocket`。
- 自定义 `baseUrl`、模型、Key、headers，沿用 Pi 的 `models.json`。
- 支持 `http://` / `https://` / `ws://` / `wss://`，自动生成 `/responses` 地址，保留网关路径前缀。
- 流式文本、推理、工具调用、图片输入、工具结果和用量解析复用 Pi AI。
- 按会话、地址与鉴权 headers 隔离连接；兼容时使用 `previous_response_id` 增量续接。
- 保留 Pi 原有工具，不接管工具执行，不修改内置 provider。
- 默认优先 WebSocket，在安全条件下自动回退同一地址的 HTTP(S)/SSE。
- 无 Sub2API、AIO 或其他网关的专用代码。

## 要求

- Node.js >= 22.19
- Node/npm 安装的 `@earendil-works/pi-coding-agent` / `pi-ai` **1.1.x**（包括当前 pi-web 内嵌的 npm 安装）
- 提供标准 Responses WebSocket `response.create` 协议和/或 HTTP(S) Responses SSE 接口的服务

本扩展使用宿主 Pi 1.1.x 的 Responses serializer/parser，不打包另一份 Pi。当前 Pi 的扩展加载器对子路径导入有别名限制，因此通过宿主物理模块路径加载这些 helper；不支持没有这些磁盘模块的独立打包二进制，不声称兼容旧 `@mariozechner/*` 包或其他 Pi 大版本。

## 安装

本地源码包先安装运行依赖，然后加入 Pi：

```powershell
cd F:\My-github\pi-responses-websocket
npm install --omit=dev --ignore-scripts --legacy-peer-deps
pi install F:\My-github\pi-responses-websocket
```

Pi 不会替本地路径包安装依赖。宿主提供 Pi 包，`--legacy-peer-deps` 避免仅运行安装时复制整套 Pi。

首次安装后重启 Pi，或 `/reload`。临时加载而不写入 Pi 配置：

```powershell
pi -e F:\My-github\pi-responses-websocket\src\index.ts
```

## 配置

将 `models.example.json` 中的 provider **合并**到 `~/.pi/agent/models.json`，不要覆盖已有 provider。

识别的是 `providers` 下的 **provider ID（对象键名）**，不是显示名称或模型 ID。以 `-ws` 或 `_ws` 结尾即可，后缀大小写不敏感；原始 ID 保持不变，不会把 `OpenAI-ws` 和 `OpenAI-WS` 合并。示例使用 `OpenAI-ws`，也可以改成 `GPT-WS` 等名称。

- 仅接管这些 provider 中 `api: "openai-responses"` 的模型，其他 API 类型不变。
- `OpenAI`、`OpenAI-ws-extra` 等不以后缀 `-ws` / `_ws` 结尾的 provider 不会被接管。
- 旧配置 `responses-websocket` 继续有效，不必迁移。
- 会话启动时自动识别；运行中通过 `/model` 加载并切换到新增加的 `*-ws` / `*_ws` provider 时也会识别。安装或更新扩展本身后仍需 `/reload` 或重启 Pi。

```json
{
  "providers": {
    "OpenAI-ws": {
      "baseUrl": "https://YOUR_GATEWAY/v1",
      "api": "openai-responses",
      "apiKey": "$RESPONSES_WS_API_KEY",
      "models": [
        {
          "id": "YOUR_MODEL_ID",
          "name": "My model (WebSocket)",
          "reasoning": true,
          "input": ["text", "image"],
          "contextWindow": 128000,
          "maxTokens": 16384,
          "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 }
        }
      ]
    }
  }
}
```

`YOUR_MODEL_ID`、输入能力、上下文窗口、输出上限和价格应按实际服务修改。示例中的窗口/上限只是示例，价格为零表示未配置价格，不代表免费。

PowerShell 中设置 Key，再从同一环境启动 Pi：

```powershell
$env:RESPONSES_WS_API_KEY = "你的网关 Key"
pi
```

也可通过 Pi 的 `auth.json` 在对应 provider ID 下保存 `api_key` 凭据，或者 `/login` 选择对应 provider（示例为 **OpenAI-ws**）输入 Key。这里的 `/login` 只是保存 API Key，**不是 OAuth**。使用 `auth.json` 时可以省略 `models.json` 的 `apiKey`。更换 provider ID 时，保存的凭据也需要使用新的 ID。

运行 `/model`，选择 `OpenAI-ws/YOUR_MODEL_ID`（名称应与配置中的 provider ID 一致）。

### 地址示例

| 场景 | `baseUrl` | 最终 WS 地址 |
|---|---|---|
| 直接网关 | `https://gateway.example/v1` | `wss://gateway.example/v1/responses` |
| 本机带前缀网关 | `http://127.0.0.1:37123/codex/v1` | `ws://127.0.0.1:37123/codex/v1/responses` |
| 已有完整地址 | `wss://gateway.example/custom/responses` | 不重复追加 `/responses` |

中间层向上游发送什么凭据由中间层配置决定，扩展只管理连接的这一跳。

### 与 openai-responses-instructions 一起使用

两个扩展可以同时加载：WS provider 会调用 Pi 的 `before_provider_request`，因此可将首条 system/developer 消息提升到顶层 `instructions`，WS 增量比较和 SSE 回退均使用转换后的请求。

`openai-responses-instructions` 的 provider 匹配条件需要同时接受 `*-ws` / `*_ws` 和旧名称 `responses-websocket`。该 companion 扩展独立维护，本插件不会修改它；如果它仅匹配 `openai` 前缀，`GPT-WS` 等名称会被跳过：

```typescript
(/^openai(?:[-_]|$)/i.test(model.provider) ||
  /[-_]ws$/i.test(model.provider) ||
  model.provider === "responses-websocket") &&
model.api === "openai-responses"
```

保留两个扩展并确认 companion 的匹配条件即可，不必手动填写 `instructions`。这项转换仍由 companion 扩展负责，WS 插件不重复实现它。

可使用实际 companion 文件运行兼容测试（包括 Pi CLI 同时加载两插件后的 WS 和 SSE 回退）：

```powershell
$env:PI_RESPONSES_WS_TEST_INSTRUCTIONS = "C:\Users\GP160_DRX\.pi\agent\extensions\openai-responses-instructions.ts"
npm test
```

### 自定义 headers / 模型兼容参数

正常使用 `Authorization: Bearer <apiKey>`。可在 provider 的 `headers` 中指定其他鉴权或路由头；由 Pi 解析环境变量，不将凭据写进 URL。

若服务明确拒绝 `max_output_tokens`，在模型中设置：

```json
{ "compat": { "supportsMaxOutputTokens": false } }
```

其他 `openai-responses` 模型兼容标志（developer role、grammar tools、strict tools 等）沿用 Pi 的实现。额外请求参数可用模型的 `samplingParams`；不应强行覆盖模型名、输入或续接引用。

## 传输行为

在 Pi 的 `settings.json` 中配置 `transport`（未指定时默认 `auto`）：

```json
{ "transport": "auto" }
```

| `transport` | 行为 |
|---|---|
| `auto` | 优先 WS 和增量续接，满足以下条件时单次回退 HTTP(S)/SSE |
| `websocket` | 强制 WS，复用连接但发送完整输入，不回退 SSE |
| `websocket-cached` | 强制 WS，可增量续接，不回退 SSE |
| `sse` | 直接使用 HTTP(S)/SSE，不尝试 WS |

回退规则：

- 握手返回 `404` / `405` / `426` / `5xx`，或建立连接失败时，可以回退。
- WS 开始返回内容前的传输故障或响应等待超时，可以回退。已收到文本、推理、工具调用或其他输出事件后，**不自动重发**，包括尚在队列中未展示的输出。
- 明确返回 `websocket_not_supported` / `unsupported_websocket_protocol` 可以回退。
- 扩展生成的增量续接收到 `previous_response_not_found` / `previous_response_id_not_found` 时，使用保存的完整历史、不带旧引用回退 SSE。调用方自带 `previous_response_id` 时不猜测完整历史，不自动回退。
- 鉴权、配额、参数、上下文长度错误，以及取消、插件关闭、hook 异常、畸形 JSON 等不会因更换传输自动重试。握手的其他 `4xx` 和重定向不回退。
- 同一次调用最多发送一次 SSE 请求；SSE 失败直接报告，不循环重试。下一次调用仍按 `transport` 选择（不永久记住回退）。Pi 自身的代理级重试由 Pi 设置管理。
- `wss/https` 回退到 **HTTPS**，本地 `ws/http` 回退到 **HTTP**；保留路径前缀、query、鉴权与完整请求。不跟随 HTTP 重定向。
- 回退记录在 assistant 消息的 `diagnostics` 中，类型为 `responses_ws_fallback`。

收到输出前失败也不能证明上游没有开始计算；回退可能产生额外用量，但不会重放已经接收的工具调用。

其他行为：

- 默认复用连接；只有请求参数一致且完整历史严格匹配上一轮输入加 Pi 可重放的输出时，才发送增量。
- `cacheRetention: "none"`（SDK 请求选项）不复用连接或发送缓存键。
- provider、Key、headers、URL 或 session 改变时不共享连接；每个匹配的 provider 使用独立连接池，并发调用使用不同连接。
- 分支切换、历史修改、提示词或工具改变时，匹配失败就发送完整输入，不猜测历史。
- 取消、异常、输出截断会丢弃连接；下次 WS 调用重新连接并使用完整输入。
- 连接和响应空闲超时读取 Pi 的 `websocketConnectTimeoutMs` / `timeoutMs` 请求选项，默认 15 秒 / 5 分钟；空闲 WS 连接 5 分钟后释放。SSE 的请求等待/事件等待也使用 `timeoutMs`。
- `onPayload` 每次逻辑调用只执行一次；回退沿用其完整结果，不重复执行变换。`onResponse` 在新 WS 握手成功时报告 101，HTTP 时报告实际状态；`onProviderStreamEvent` 收到原始事件。
- WS 使用 Node `ws` 直接连接，不使用 `fetch`；HTTP 使用 `options.fetch` 或全局 `fetch`。扩展不额外配置 `HTTP_PROXY` / `HTTPS_PROXY`，实际 HTTP 代理行为由宿主 fetch 决定。

支持标准 Responses 协议并不等于实现某个网关特有的会话恢复、路由 nonce 或 Codex 全套功能。本版不处理这些专用协议，也不提供后台生成、预热、原生压缩、搜索或生图工具。

## 开发与验证

```powershell
npm install --include=dev --ignore-scripts --legacy-peer-deps
npm run check
npm test
npm pack --dry-run
```

测试仅使用 loopback WebSocket 服务、临时 Pi 配置和合成 Key，不读取用户凭据，不向生产模型发送请求。覆盖文本/Unicode、推理、用量、工具增量与全量重放、图片输入与工具图片结果、跨 provider 历史、连接隔离、hooks、取消、超时、握手拒绝和未完成工具调用。

本机验证：`npm run check` 通过，`npm test` **89/89 通过**。包括大小写不敏感的 `*-ws` / `*_ws` 识别、运行中模型切换注册、连接池关闭，以及实际 Pi CLI 对 `OpenAI-ws` / `OpenAI-WS` / `GPT-WS` / `custom-Ws` 及下划线版本的 WS 请求和 SSE 回退。普通 `OpenAI` 和 `OpenAI-ws-extra` 仍使用原有 HTTP 路径；旧名称 `responses-websocket` 保持兼容。本轮未设置可选的 `PI_RESPONSES_WS_TEST_INSTRUCTIONS`，未运行实际 companion 扩展兼容测试。

`PI_RESPONSES_WS_TEST_CLI` 可指定另一个 Pi `dist/cli.js` 路径，验证实际宿主加载。未进行真实 Sub2API/AIO/官方 API 联调；部署端是否启用 WebSocket、模型/配额是否可用仍由部署配置决定。

## 卸载

```powershell
pi remove F:\My-github\pi-responses-websocket
```

之后可手动移除 `models.json` 中对应的 `*-ws` / `*_ws` 或旧 `responses-websocket` 配置及对应凭据。扩展自身不会写入用户配置。
