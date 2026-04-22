<p align="center">
  <img src="dashboard/assets/aitd-readme-header.png" alt="AITD - AI Trading Agent for Everyone" width="520">
</p>

<p align="center">
  <strong>中文</strong> | <a href="./README_EN.md">English</a>
</p>

# AITD - AI Trading Agent

🤖 AITD 是一个能让你在任何机器上部署AI交易机器人项目。可以在浏览器里完成配置、模拟盘验证和实盘运行。

<p align="center">
  <img src="docs/overview.png" width="1200">
</p>

## 项目特色：
- ✍️ 完全可自定义交易 Prompt，最大化你的策略表现
  - 可以定义AI交易员性格、交易核心原则、入场条件、仓位管理
  - 支持将K线数据加入prompt辅助AI交易员决策
  - 支持保存prompt, 加载、修改已保存prompt
- 🚀 支持动态交易池，方便自定义交易池
  - 方便引用动态交易池，实时抓住热点品种
- 📈 支持自定义交易周期
- 🧠 实时查看AI交易员输入与决策
  - 检查每轮AI交易员的输入信息是否符合预期
  - 检查每轮AI交易员的交易操作是否符合预期
- 🕹️ 丰富的组合风控设置，详细的交易记录
- ⚙️ 一键切换模拟盘与实盘
- 🦞 支持主流AI模型Provider
- 🔑 完全本地运行，确保账户、prompt等信息的绝对安全

当前支持交易所

1. Crypto:
- ✅ Binance
- OKX (todo)
- BYBIT (todo)
- Gate (todo)
  
2. Futures:
- CTP (todo)
- IBKR (todo)



## 运行要求
✅ 只需要你的机器有`Python 3.11+`就可以完美运行

## 快速开始

下载后在项目根目录运行：

```bash
python3 run.py
```

也可以显式指定端口：

```bash
python3 run.py --port 1234
```

然后浏览器打开终端里打印出来的地址（默认端口号为8788），例如：

```text
http://127.0.0.1:8788/trader.html
```

## 第一次使用建议
- 准备好LLM API_KEY
- 准备好交易账户API和Secret, 在设置中将本机 IP 加入白名单（看各自交易所要求）

1. 先进入 `模拟盘` 页面
2. 在 `AI模型配置` 里填写 `Provider / Model / API Key / Base URL`
3. 如果需要代理，在 `代理配置` 里开启并填写代理地址
4. 在 `Prompt` 页面编辑交易逻辑并进行测试，确保AI模型工作正常
5. 在 `候选池` 页面选择：
   - `静态候选池`：手动输入想要交易标的 symbols
   - `动态候选池`：写 Python function 自动获取动态 symbols
6. 点击当前页面的 `启动交易`。
7. 在 `交易 / Prompt / Log` 里确认是否正常工作
8. 模拟盘完整跑通后，配置实盘账号，启动实盘交易

## 功能概览

- `交易` 显示账户、权益曲线、当前持仓、候选池和最近决策
<p align="center">
  <img src="docs/portfolio.png" width="1200">
</p>

- `Prompt` 让用户修改交易逻辑，仓位管理逻辑，并测试 Prompt 是否能正常工作
<p align="center">
  <img src="docs/custom_prompt.png" width="1200">
</p>

- `候选池` 支持静态 symbols 和动态 Python function 二选一
<p align="center">
  <img src="docs/dynamic_asset_universe.png" width="1200">
</p>

- `Log` 显示当前进程的运行日志和错误信息


## 关键文件

- [run.py](./run.py)
  启动入口
- [dashboard/](./dashboard)
  浏览器界面资源
- [config/](./config)
  默认配置文件

---

## 稳定代理推荐🪜
实测 🇨🇳/🇺🇸 均可使用
[MOJIE](https://mojie.me/#/register?code=6seDUwEZ)

## 联系方式

- X: [@ywa_ywa_ywa](https://x.com/ywa_ywa_ywa)

## 版权与使用

Copyright © 2026 Yaolin Wang. All rights reserved.

本仓库当前仅用于评估、学习和个人参考。
任何商业使用、重新分发、托管部署或付费服务集成，均需事先获得书面许可。

如需商用授权，请通过上方联系方式联系。
