import sys
import os
import asyncio
import time
import argparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src", "hydrakv")))

from hydrakv import Hydrakv


async def benchmark_op(hc, db_name, op_type, keys, values, concurrency):
    sem = asyncio.Semaphore(concurrency)
    latencies = []

    async def one_op(i):
        async with sem:
            start = time.perf_counter()
            if op_type == "set":
                await hc.set(db_name, keys[i], values[i])
            elif op_type == "get":
                await hc.get(db_name, keys[i])
            elif op_type == "delete":
                await hc.delete(db_name, keys[i])
            latencies.append(time.perf_counter() - start)

    tasks = [asyncio.create_task(one_op(i)) for i in range(len(keys))]
    await asyncio.gather(*tasks)
    return latencies


async def run_benchmark(host, port, grpc_port, num_ops, concurrency):
    hc = Hydrakv(
        host,
        port,
        grpc_port=grpc_port,
        use_grpc=True,
        log_lvl="ERROR",
    )

    db_name = "benchdb"
    try:
        await hc.create_db(db_name)
    except Exception:
        pass

    # Pre-generate keys & values
    keys = [f"k{i}" for i in range(num_ops)]
    values = [f"v{i}" for i in range(num_ops)]

    print(f"Running benchmark: ops={num_ops}, concurrency={concurrency}")

    for op in ("set", "get", "delete"):
        print(f"\nBenchmarking {op.upper()}")

        start = time.perf_counter()
        latencies = await benchmark_op(
            hc, db_name, op, keys, values, concurrency
        )
        total = time.perf_counter() - start

        avg_lat = (sum(latencies) / len(latencies)) * 1000
        throughput = len(latencies) / total

        print(f"Total time: {total:.4f} s")
        print(f"Throughput: {throughput:.2f} ops/s")
        print(f"Average latency: {avg_lat:.3f} ms")

    try:
        await hc.delete_db(db_name)
    except Exception:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="192.168.68.100")
    parser.add_argument("--port", type=int, default=9191)
    parser.add_argument("--grpc-port", type=int, default=9292)
    parser.add_argument("--num-ops", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=10)

    args = parser.parse_args()

    asyncio.run(
        run_benchmark(
            args.host,
            args.port,
            args.grpc_port,
            args.num_ops,
            args.concurrency,
        )
    )
