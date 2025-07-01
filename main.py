import argparse
import csv
import operator
from tabulate import tabulate
from typing import List, Dict, Callable, Optional, Any


def read_csv(file_path: str) -> List[Dict[str, str]]:
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)


def filter_data(data: List[Dict[str, str]], condition: str) -> List[Dict[str, str]]:
    ops: Dict[str, Callable[[Any, Any], bool]] = {
        '>': operator.gt,
        '<': operator.lt,
        '=': operator.eq
    }

    for op_str in ops:
        if op_str in condition:
            column, value = condition.split(op_str)
            column = column.strip()
            value = value.strip()
            op_func = ops[op_str]
            break
    else:
        raise ValueError("Invalid filter condition")

    try:
        value = float(value)
        return [row for row in data if op_func(float(row[column]), value)]
    except ValueError:
        return [row for row in data if op_func(row[column], value)]


def try_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except ValueError:
        return None


class Aggregator:
    def __init__(self):
        self.functions: Dict[str, Callable[[List[float]], float]] = {
            'avg': lambda x: sum(x) / len(x) if x else 0,
            'min': min,
            'max': max,
            # можно быстро добавить: 'median': lambda x: sorted(x)[len(x)//2] и т.д.
        }

    def apply(self, func: str, values: List[float]) -> float:
        if func not in self.functions:
            raise ValueError(f"Unsupported aggregate function: {func}")
        return self.functions[func](values)

    def all(self, values: List[float]) -> List[List[str]]:
        return [[key, f'{self.functions[key](values):.2f}'] for key in self.functions]



def aggregate_all_columns(data: List[Dict[str, str]]) -> None:
    numeric_columns: List[str] = []
    for key in data[0].keys():
        if all(try_float(row[key]) is not None for row in data):
            numeric_columns.append(key)

    aggregator = Aggregator()
    result_table: List[List[str]] = []
    for column in numeric_columns:
        values = [float(row[column]) for row in data]
        result_table.extend([[func, column, val] for func, val in aggregator.all(values)])

    print(tabulate(result_table, headers=['func', 'column', 'value'], tablefmt='grid'))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', required=True)
    parser.add_argument('--where')
    parser.add_argument('--aggregate')
    args = parser.parse_args()

    data = read_csv(args.file)

    if args.where:
        data = filter_data(data, args.where)

    if not data:
        print("No data matched the filter condition.")
        return

    print(tabulate(data, headers='keys', tablefmt='grid'))

    if args.aggregate:
        if '=' not in args.aggregate:
            print("Invalid aggregate argument. Use column=func format.")
            return
        column, func = args.aggregate.split('=')
        column = column.strip()
        func = func.strip()

        try:
            values = [float(row[column]) for row in data]
        except ValueError:
            print("Aggregation can only be applied to numeric columns.")
            return

        aggregator = Aggregator()
        if func == 'all':
            result = aggregator.all(values)
            print(tabulate([[f, column, v] for f, v in result], headers=['func', 'column', 'value'], tablefmt='grid'))
        else:
            result = aggregator.apply(func, values)
            print(tabulate([[f'{result:.2f}']], headers=[func], tablefmt='grid'))
    else:
        aggregate_all_columns(data)


if __name__ == '__main__':
    main()
