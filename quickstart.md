# Quickstart

This local pilot lets one meeting assistant use **OpenAI Agents SDK, Google ADK, CrewAI or LangGraph**. You can change runtimes during the meeting while keeping the same bot, shared tools and conversation history. All four use the same bridge.

MeetStream runs the meeting bot, transports audio and delivers chat. The bridge connects the chosen runtime and streams its answer into speech. This uses MeetStream's existing external-agent connection and needs no MIA backend changes. Hosted MIA custom functions remain an optional mode.

## 1. Install once

Extract the project into a folder you can write to. Install [Node.js LTS](https://nodejs.org/) (includes npm) and the current [uv](https://docs.astral.sh/uv/getting-started/installation/) (0.13.4 or newer). Reopen your terminal. Python downloads automatically; you do not need to run `npm install`.

You need:

- A [MeetStream API key](https://app.meetstream.ai/api-key).
- An [OpenAI API key](https://platform.openai.com/api-keys) for OpenAI Agents SDK.
- A [Google model key](https://aistudio.google.com/apikey) for Google ADK and the pilot's Google-backed CrewAI and LangGraph.
- A [Deepgram key](https://console.deepgram.com/) for listening and speaking.
- An [ngrok authtoken](https://dashboard.ngrok.com/get-started/your-authtoken), so MeetStream can reach the local bridge.
- A Google Meet meeting that you are allowed to record and join with a bot. Google Meet is the validated pilot platform; Zoom and Teams links are accepted but live voice behaviour has not been validated there.

Have both model keys ready to switch between all four runtimes. MeetStream manages the meeting, the selected model provider powers reasoning, Deepgram handles listening and speaking, and ngrok connects the meeting service to your computer. Do not paste keys into meeting chat.

## 2. Start

Open a terminal in the project folder:

```sh
npm start
```

The first start uses ADK. Later starts remember your selected runtime. All four runtimes are available. To choose ADK at startup, use:

```sh
npm start -- --runtime adk
```

The first start may take several minutes. Paste missing keys when asked; they are hidden while typing and saved privately in `.env`. Paste your meeting link when asked. The bridge sends **Adapter SDK Test** into the meeting automatically. Admit it if your meeting requires host approval, then wait for **Assistant connected. You can speak now.** Keep the terminal open.

For a terminal without interactive input, first save the required credentials through interactive setup or provide them as environment variables:

```sh
npm start -- --meeting https://meet.google.com/abc-defg-hij
```

Replace the example with your real link. Normal startup creates one assistant and never starts a scripted test speaker. No MeetStream dashboard launch or stop is needed.

## 3. Speak and switch

Say **“Okay assistant”** before a request. For example:

- “Okay assistant, look up project adapter status.”
- “Okay assistant, create an action item titled Review notes assigned to Alex.”
- “Okay assistant, switch to LangGraph.”
- “Okay assistant, stop.”

Use the same switch command with OpenAI, ADK, CrewAI or LangGraph. The meeting chat confirms the switch; the next request uses that runtime with the same meeting bot, shared history and tools. Speech can be interrupted while an answer is playing. Saying “stop” interrupts the answer and keeps the bot in the meeting; use Ctrl+C or `npm stop` to make it leave. The included project lookup and action-item tools are sandbox demonstrations.

You can pronounce ADK as “A D K” and CrewAI as “crew A I.” Common transcriptions such as “a dee kay,” “crew eye” and “land graph” are recognized within a switch command. If a runtime name is unclear, the bot asks in chat and keeps the current runtime.

To choose the starting runtime, use one of these commands:

| Runtime | Command | Model key used at startup |
|---|---|---|
| OpenAI Agents SDK | `npm start -- --runtime openai` | `OPENAI_API_KEY` |
| Google ADK | `npm start -- --runtime adk` | `GOOGLE_API_KEY` |
| CrewAI | `npm start -- --runtime crewai` | `GOOGLE_API_KEY` |
| LangGraph | `npm start -- --runtime langgraph` | `GOOGLE_API_KEY` |

Startup asks for missing credentials and remembers your selection. The selected provider handles requests; provider errors are reported without silently switching to another provider.

## 4. Stop

Press **Ctrl+C** in the startup window, or open another terminal in the project folder:

```sh
npm stop
```

The bridge cancels its work, makes its bot leave and closes the local connection. Other meeting participants stay. Wait for the stopped message. Repeated stop is safe. If network problems prevent departure, cleanup remains saved; run `npm stop` again when connectivity returns. Keep `.adapter` and your credentials until cleanup is confirmed. Stop also reconciles saved owned bots after a process crash.

## Optional hosted MIA tool mode

```sh
npm start -- --mode mia
```

This automatically registers a Google-backed hosted MIA agent and joins your meeting. Gemini must already be connected under MeetStream **Integrations → Models**. Speak normally and say **“Switch the external agent to LangGraph”** to switch. Stop makes the bot leave and removes its owned registration.

This mode returns completed tool answers to hosted MIA for its own speech. Choose the default streaming path when incremental external-agent speech is required. Both modes use the same four runtime adapters, tool executor and session host.

## Help

| Problem | Action |
|---|---|
| `npm` or `uv` not found | Install the tools above and reopen the terminal. |
| Assistant is waiting | Admit the bot from your meeting's waiting room. |
| Missing key | Run interactively and paste the requested key privately. |
| Provider quota or authentication error | Check the selected provider's credits, key and model access. |
| Bridge already running | Run `npm stop` in this project folder. |
| Cloud cleanup pending | The bot may already have left while MeetStream still reports it as active. Keep `.adapter` and credentials; retry `npm stop` once the service updates its status. |

See the [README](README.md) for workflow and architecture, and [validation](docs/validation.md) for recorded checks and limits.
