import argparse
import json
import tempfile

from .demo import run, run_backfill


def main():
    parser = argparse.ArgumentParser(description='按地区业务日运行的活动数仓作业')
    parser.add_argument('--root', help='持久化数据目录；省略时使用隔离临时目录')
    parser.add_argument('--scenario', choices=('explicit', 'backfill'), default='explicit',
                        help='explicit=原有显式输入演示；backfill=按真实输入依赖的补算演示')
    args = parser.parse_args()
    scenario = run_backfill if args.scenario == 'backfill' else run
    if args.root:
        result = scenario(args.root)
    else:
        with tempfile.TemporaryDirectory() as directory:
            result = scenario(directory)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == '__main__':
    main()
