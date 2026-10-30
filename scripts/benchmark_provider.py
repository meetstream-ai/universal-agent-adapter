"""Explicit provider-only, no-tool latency sampling. This consumes provider usage."""
import argparse
import asyncio
import json
import logging
import math
import time
from pathlib import Path

from meetstream_agent_adapter.adapters import load_adapter
from meetstream_agent_adapter.cli import load_environment
from meetstream_agent_adapter.contract import EventType, SessionContext, Turn
from meetstream_agent_adapter.host import MemoryOutput, SessionHost
from meetstream_agent_adapter.tools import ToolExecutor


class TimedOutput(MemoryOutput):
    def __init__(self):
        super().__init__()
        self.started, self.first = 0, None

    async def event(self, event, active):
        if event.type == EventType.DELTA and self.first is None:
            self.first = time.perf_counter() - self.started
        await super().event(event, active)


def percentile(values, percent):
    return round(sorted(values)[math.ceil(len(values) * percent / 100) - 1] * 1000, 3) if values else None


async def run(args):
    reports = []
    for profile in args.profiles:
        model = ('gemini/' if profile == 'crewai' else '') + args.model
        tools, output = ToolExecutor([], set()), TimedOutput()
        host = await SessionHost.create(SessionContext(session_id='latency-' + profile, workspace='benchmark', bot_id='none', profile=profile),
                                        load_adapter(profile, model=model, **({'provider': 'google'} if profile == 'langgraph' else {})), tools, output)
        report = {'profile':profile, 'model':model, 'kind':'real_provider_no_meeting_no_tools',
                  'samples_requested':args.samples, 'samples_completed':0, 'interval_seconds':args.interval,
                  'client_timezone':time.strftime('%Z%z'), 'provider_region':'not exposed', 'passed':False}
        latencies, firsts = [], []
        try:
            for n in range(args.samples):
                output.started, output.first = time.perf_counter(), None
                await host.accept(Turn(event_id=f'sample-{n}', text='Say ready.', timestamp=n))
                await host.wait()
                if host.events[-1].type != EventType.COMPLETED or output.first is None:
                    report['error_code'] = host.events[-1].payload.get('code', 'missing_text')
                    break
                latencies.append(time.perf_counter() - output.started)
                firsts.append(output.first)
                report['samples_completed'] = len(latencies)
                if (n + 1) % 10 == 0:
                    print(json.dumps({'profile':profile, 'samples_completed':len(latencies)}), flush=True)
                if n + 1 < args.samples:
                    await asyncio.sleep(max(0, args.interval - (time.perf_counter() - output.started)))
            report['passed'] = len(latencies) == args.samples
            report['turn_ms'] = {'p50':percentile(latencies,50), 'p95':percentile(latencies,95)}
            report['first_text_ms'] = {'p50':percentile(firsts,50), 'p95':percentile(firsts,95)}
            report['history_messages'] = len(host.history)
            report['retained_events'] = len(host.events)
            report['tool_calls'] = host.metrics['tool.started']
        finally:
            await host.close()
            tools.close()
        reports.append(report)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({'profiles':reports}, indent=2) + '\n')
        print(json.dumps(report), flush=True)
    return all(report['passed'] for report in reports)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--env-file', default='.env')
    parser.add_argument('--profiles', nargs='+', choices=['adk','crewai','langgraph'], default=['adk','crewai'])
    parser.add_argument('--model', default='gemini-3.5-flash-lite')
    parser.add_argument('--samples', type=int, default=100)
    parser.add_argument('--interval', type=float, default=8)
    parser.add_argument('--output', type=Path, default=Path('reports/live-provider-latency.json'))
    args = parser.parse_args()
    if not 1 <= args.samples <= 1000 or args.interval < 1:
        parser.error('Use 1–1000 samples and an interval of at least one second')
    logging.disable(logging.CRITICAL)
    load_environment(args.env_file)
    raise SystemExit(0 if asyncio.run(run(args)) else 1)
