import argparse
import time
import threading

try:
    # prefer local package imports when used as a module
    from medallion.gold.transform_gold import build_gold_layer
    from medallion.gold.dim import stream_dim_from_gold
except Exception:
    # allow running directly from medallion/gold by adjusting sys.path
    import os
    import sys
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from medallion.gold.transform_gold import build_gold_layer
    from medallion.gold.dim import stream_dim_from_gold


def main():
    p = argparse.ArgumentParser(description='Launch gold transform and dim pipelines')
    p.add_argument('--interval', type=int, default=1, help='poll interval seconds for both pipelines')
    p.add_argument('--both', action='store_true', help='run both transform_gold and dim concurrently')
    p.add_argument('--gold-only', action='store_true', help='run only the gold transformer')
    p.add_argument('--dim-only', action='store_true', help='run only the dim pipeline')
    args = p.parse_args()

    if args.dim_only:
        stream_dim_from_gold(poll_interval_seconds=args.interval)
        return

    if args.gold_only:
        build_gold_layer(poll_interval_seconds=args.interval)
        return

    # default: run both (or only if --both set)
    if args.both or (not args.gold_only and not args.dim_only):
        t_gold = threading.Thread(target=build_gold_layer, kwargs={'poll_interval_seconds': args.interval}, daemon=True)
        t_dim = threading.Thread(target=stream_dim_from_gold, kwargs={'poll_interval_seconds': args.interval}, daemon=True)
        t_gold.start()
        t_dim.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print('Interrupted; shutting down both algorithms...')


if __name__ == '__main__':
    main()
