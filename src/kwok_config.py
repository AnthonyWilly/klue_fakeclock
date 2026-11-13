"""Validate capacity overrides and overlay the installed KWOK configuration."""
import argparse
import copy
import json
import sys

WORKERS = ('podPlayStageParallelism', 'nodePlayStageParallelism', 'nodeLeaseParallelism')


def read_workers(path):
    if not path:
        return {}
    import yaml
    try:
        with open(path, encoding='utf-8') as stream:
            values = yaml.safe_load(stream)
    except (OSError, yaml.YAMLError) as error:
        raise ValueError(f'invalid KWOK workers configuration: {error}') from error
    if not isinstance(values, dict) or not values or set(values) - set(WORKERS):
        raise ValueError('workers configuration must contain only supported parallelism fields')
    if any(type(value) is not int or value < 1 or value > 2**32 - 1 for value in values.values()):
        raise ValueError('KWOK worker counts must be positive uint32 integers')
    return values


def merge_workers(config_map, overrides):
    import yaml
    result = copy.deepcopy(config_map)
    documents = list(yaml.safe_load_all(result['data']['kwok.yaml']))
    configs = [doc for doc in documents if isinstance(doc, dict) and
               doc.get('apiVersion') == 'config.kwok.x-k8s.io/v1alpha1' and doc.get('kind') == 'KwokConfiguration']
    if len(configs) != 1 or not isinstance(configs[0].get('options'), dict):
        raise ValueError('installed KWOK config does not contain one supported KwokConfiguration')
    configs[0]['options'].update(overrides)
    result['data']['kwok.yaml'] = yaml.safe_dump_all(documents, sort_keys=False)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('validate', 'merge', 'ksm'))
    parser.add_argument('--workers-config')
    parser.add_argument('--speed-factor', default='1')
    args = parser.parse_args()
    try:
        overrides = read_workers(args.workers_config)
        if args.action == 'merge':
            json.dump(merge_workers(json.load(sys.stdin), overrides), sys.stdout)
        elif args.action == 'ksm':
            from sampling import SamplingIntervals, configure_ksm
            json.dump(configure_ksm(json.load(sys.stdin), SamplingIntervals(args.speed_factor).ksm), sys.stdout)
    except (ValueError, KeyError, TypeError) as error:
        parser.error(str(error))


if __name__ == '__main__':
    main()
