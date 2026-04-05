# ************************************************************************* #
#                                                                           #
#                                                      :::      ::::::::    #
#  data_pipeline.py                                  :+:      :+:    :+:    #
#                                                  +:+ +:+         +:+      #
#  By: klucchin <klucchin@student.42.fr>         +#+  +:+       +#+         #
#                                              +#+#+#+#+#+   +#+            #
#  Created: 2026/04/05 20:16:01 by klucchin        #+#    #+#               #
#  Updated: 2026/04/05 22:59:09 by klucchin        ###   ########.fr        #
#                                                                           #
# ************************************************************************* #

from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Protocol


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: List[str] = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> Tuple[int, str]:
        if not self._data:
            raise Exception("No data available")
        value = self._data.pop(0)
        return (0, value)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(x, (int, float)) for x in data)
        return False

    def ingest(self, data: int | float | List[int | float]) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")

        if isinstance(data, list):
            for x in data:
                self._data.append(str(x))
        else:
            self._data.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: str | List[str]) -> None:
        if not self.validate(data):
            raise Exception("Improper text data")

        if isinstance(data, list):
            self._data.extend(data)
        else:
            self._data.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        def is_valid_log(d: dict[str, str]) -> bool:
            return (
                isinstance(d, dict)
                and all(isinstance(i, str) for i in d.keys())
                and all(isinstance(j, str) for j in d.values())
            )

        if is_valid_log(data):
            return True
        if isinstance(data, list):
            return all(is_valid_log(d) for d in data)
        return False

    def ingest(self, data: dict[str, str] | List[dict[str, str]]) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")

        def format_log(d: dict[str, str]) -> str:
            return f"{d.get('log_level', '')}: {d.get('log_message', '')}"

        if isinstance(data, list):
            for d in data:
                self._data.append(format_log(d))
        else:
            self._data.append(format_log(data))


class ExportPlugin(Protocol):
    def process_output(self, data: List[Tuple[int, str]]) -> None:
        pass


class CSVExportPlugin:
    def process_output(self, data: List[Tuple[int, str]]) -> None:
        values = [value for _, value in data]
        print("CSV Output:")
        print(",".join(values))


class JSONExportPlugin:
    def process_output(self, data: List[Tuple[int, str]]) -> None:
        result = {}

        for i, (_, value) in enumerate(data):
            result[f"item_{i}"] = value

        print("JSON Output:")
        items = [f'"{k}": "{v}"' for k, v in result.items()]
        print("{" + ", ".join(items) + "}")


class DataStream:
    def __init__(self) -> None:
        self._processors: List[DataProcessor] = []
        self._stats: dict[str, int] = {}

    def register_processor(self, proc: DataProcessor) -> None:
        name = type(proc).__name__
        self._processors.append(proc)
        self._stats[name] = 0

    def process_stream(self, stream: List[Any]) -> None:
        for element in stream:
            handled = False

            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)

                    name = type(proc).__name__
                    if isinstance(element, list):
                        self._stats[name] += len(element)
                    else:
                        self._stats[name] += 1

                    handled = True
                    break

            if not handled:
                print(
                    "DataStream error - "
                    f"Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")

        if not self._processors:
            print("No processor found, no data")
            return

        for proc in self._processors:
            name = type(proc).__name__
            total = self._stats[name]
            remaining = len(proc._data)

            print(f"{name.replace('Processor', ' Processor')}: total {total} "
                  f"items processed, remaining {remaining} on processor"
                  )

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            collected = []

            for _ in range(nb):
                try:
                    item = proc.output()
                    collected.append(item)
                except Exception:
                    break

            if collected:
                plugin.process_output(collected)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===\n")

    print("Initialize Data Stream...")
    data = DataStream()
    data.print_processors_stats()

    print("\nRegistering Processors")
    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    data.register_processor(num)
    data.register_processor(text)
    data.register_processor(log)

    stream1 = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING',
             'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO',
             'log_message': 'User wil is connected'}
        ],
        42,
        ["Hi", "five"]
    ]

    print("\nSend first batch of data on stream:", stream1)
    data.process_stream(stream1)
    print()
    data.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    data.output_pipeline(3, CSVExportPlugin())

    print()
    data.print_processors_stats()

    stream2 = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {'log_level': 'ERROR', 'log_message': '500 server crash'},
            {'log_level': 'NOTICE',
             'log_message': 'Certificate expires in 10 days'}
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello"
    ]

    print("\nSend another batch of data:", stream2)
    data.process_stream(stream2)
    print()
    data.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    data.output_pipeline(5, JSONExportPlugin())

    print()
    data.print_processors_stats()


if __name__ == "__main__":
    main()
