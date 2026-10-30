"""Native SDK stress/latency checks with controlled models, never provider claims.

Requires the dev and framework extras. Reuses the shared test-only model support;
production adapters and native framework runners are unchanged.
"""

import argparse
import asyncio
import importlib.util
import json
import logging
import math
import time
from pathlib import Path

from meetstream_agent_adapter.contract import EventType, Turn

FIXTURE_PATH = Path(__file__).resolve().parents[1] / 'tests' / 'framework_models.py'


def percentile(values, percent):
    return round(sorted(values)[max(0, math.ceil(len(values) * percent / 100) - 1)] * 1000, 3)


async def benchmark(profile, samples, cancellations):
    spec = importlib.util.spec_from_file_location('controlled_models', FIXTURE_PATH)
    fixtures = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixtures)
    host, tools, model, writes = await fixtures.build(profile)
    durations, stops = [], []
    n = 0
    try:
        # The model fixture's first invocation exercises one shared write. The
        # measured subsequent no-tool turns deliberately do not replay it.
        await host.accept(Turn(event_id='warmup', text='Warm up', timestamp=0))
        await host.wait()
        assert host.events[-1].type == EventType.COMPLETED and len(writes) == 1
        for n in range(1, samples + 1):
            started = time.perf_counter()
            await host.accept(Turn(event_id=f'sample-{n}', text='Give the answer', timestamp=n))
            await host.wait()
            durations.append(time.perf_counter() - started)
            assert host.events[-1].type == EventType.COMPLETED
        for cycle in range(cancellations):
            model.blocking = True
            model.waiting.clear()
            model.cleaned.clear()
            n += 1
            await host.accept(Turn(event_id=f'cancel-{cycle}', text='Begin response', timestamp=n))
            async with asyncio.timeout(5):
                await model.waiting.wait()
            started = time.perf_counter()
            outcome = await host.interrupt()
            stops.append(time.perf_counter() - started)
            assert outcome.runtime_stopped and outcome.state_safe and model.cleaned.is_set()
            assert host.events[-1].type == EventType.CANCELLED
            model.blocking = False
            n += 1
            await host.accept(Turn(event_id=f'recover-{cycle}', text='Next response', timestamp=n))
            await host.wait()
            assert host.events[-1].type == EventType.COMPLETED
        assert len(writes) == 1 and len(host.history) <= 40 and len(host.events) <= 512
        return {'profile': profile, 'model': 'controlled conformance model', 'region': 'local process',
                'kind': 'native_sdk_controlled_model_stress', 'no_tool_turns': samples,
                'cancel_and_recovery_cycles': cancellations, 'sandbox_writes_including_warmup': len(writes),
                'turn_ms': {'p50': percentile(durations, 50), 'p95': percentile(durations, 95)},
                'cancellation_ms': {'p50': percentile(stops, 50), 'p95': percentile(stops, 95)},
                'history_messages': len(host.history), 'retained_events': len(host.events), 'passed': True}
    finally:
        await host.close()
        tools.close()


async def main(args):
    reports = []
    for profile in args.profiles:
        report = await benchmark(profile, args.samples, args.cancellations)
        reports.append(report)
        print(json.dumps(report), flush=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({'scope': 'Actual SDK runners; controlled model latency includes fixture delays. No provider, network or audible playback latency is claimed.',
                                      'profiles': reports}, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--profiles', nargs='+', choices=['openai', 'langgraph', 'adk', 'crewai'], default=['openai', 'langgraph', 'adk', 'crewai'])
    parser.add_argument('--samples', type=int, default=100)
    parser.add_argument('--cancellations', type=int, default=10)
    parser.add_argument('--output', type=Path, default=Path('reports/native-stress.json'))
    args = parser.parse_args()
    if args.samples < 1 or args.cancellations < 1:
        parser.error('Positive sample and cancellation counts are required')
    logging.disable(logging.CRITICAL)
    asyncio.run(main(args))
